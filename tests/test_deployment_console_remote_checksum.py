from __future__ import annotations

import errno
import hashlib
import io
import shlex
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from services.deploy.console_backend import ParamikoRemote


ROOT = "/home/csda/AI_Routing"


class _StatOnlySFTP:
    def __init__(
        self, error: OSError | None = None, *, leaf_mode: int = 0o100640
    ) -> None:
        self.error = error
        self.leaf_mode = leaf_mode
        self.stat_calls = 0
        self.open_calls = 0
        self.put_calls = 0

    def stat(self, _path: str) -> SimpleNamespace:
        self.stat_calls += 1
        if self.error is not None:
            raise self.error
        return SimpleNamespace(st_size=123, st_mode=0o100640)

    def lstat(self, _path: str) -> SimpleNamespace:
        if self.error is not None:
            raise self.error
        return SimpleNamespace(st_size=123, st_mode=self.leaf_mode)

    def open(self, *_: object) -> None:
        self.open_calls += 1
        raise AssertionError("Guard failure must happen before an SFTP read/write.")

    def put(self, *_: object) -> None:
        self.put_calls += 1
        raise AssertionError("Guard failure must happen before an SFTP upload.")


class _MemoryWriter(io.BytesIO):
    def __init__(self, sftp: "_MemorySFTP", path: str) -> None:
        super().__init__()
        self.sftp = sftp
        self.path = path

    def __exit__(self, *_: object) -> None:
        self.sftp.files[self.path] = self.getvalue()
        return None

    def write(self, value: bytes) -> int:
        self.sftp.events.append(("write", self.path, len(value)))
        return super().write(value)


class _MemorySFTP:
    def __init__(self) -> None:
        self.directories = {ROOT, ROOT + "/shared"}
        self.files: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}
        self.events: list[tuple[object, ...]] = []

    def stat(self, path: str) -> SimpleNamespace:
        if path in self.files:
            return SimpleNamespace(
                st_size=len(self.files[path]),
                st_mode=stat.S_IFREG | self.modes.get(path, 0o640),
            )
        if path in self.directories:
            return SimpleNamespace(st_size=0, st_mode=stat.S_IFDIR | 0o750)
        raise FileNotFoundError(errno.ENOENT, "missing", path)

    def lstat(self, path: str) -> SimpleNamespace:
        return self.stat(path)

    def mkdir(self, path: str, mode: int = 0o750) -> None:
        if str(Path(path).parent).replace("\\", "/") not in self.directories:
            raise FileNotFoundError(errno.ENOENT, "parent missing", path)
        self.directories.add(path)

    def open(self, path: str, mode: str):
        self.events.append(("open", path, mode))
        if mode == "rb":
            return io.BytesIO(self.files[path])
        if mode == "wb":
            return _MemoryWriter(self, path)
        if mode == "wx":
            if path in self.files:
                raise FileExistsError(errno.EEXIST, "exists", path)
            self.files[path] = b""
            return _MemoryWriter(self, path)
        raise AssertionError(mode)

    def put(self, local: str, target: str) -> None:
        self.files[target] = Path(local).read_bytes()

    def posix_rename(self, source: str, target: str) -> None:
        self.files[target] = self.files.pop(source)
        if source in self.modes:
            self.modes[target] = self.modes.pop(source)

    def rename(self, source: str, target: str) -> None:
        self.posix_rename(source, target)

    def remove(self, path: str) -> None:
        del self.files[path]

    def chmod(self, path: str, mode: int) -> None:
        self.events.append(("chmod", path, mode))
        self.modes[path] = mode


class ParamikoRemoteChecksumTests(unittest.TestCase):
    def setUp(self) -> None:
        self.remote = ParamikoRemote({"remote_root": ROOT})
        self.remote.sftp = _StatOnlySFTP()

    @staticmethod
    def _guard_result(path: str, *, canonical: str | None = None) -> tuple[int, str, str]:
        return 0, f"{ROOT}\n{canonical or path}\n", ""

    def test_sha256_uses_safely_quoted_server_command_without_sftp_stream(self) -> None:
        path = ROOT + "/shared/space 'and; punctuation.csv"
        digest = "A" * 64
        expected_command = f"sha256sum -- {shlex.quote(path)}"
        calls: list[tuple[str, int]] = []

        def execute(command: str, timeout: int = 45) -> tuple[int, str, str]:
            calls.append((command, timeout))
            if command.startswith("canonical_root="):
                return self._guard_result(path)
            return 0, f"{digest}  {path}\n", ""

        with mock.patch.object(self.remote, "execute", side_effect=execute):
            self.assertEqual(self.remote.sha256(path), digest.lower())
        self.assertEqual(calls[-1], (expected_command, 45))
        self.assertTrue(calls[0][0].startswith("canonical_root=$(readlink -f -- "))
        self.assertEqual(self.remote.sftp.open_calls, 0)

    def test_sha256_returns_none_only_when_initial_sftp_stat_is_enoent(self) -> None:
        self.remote.sftp = _StatOnlySFTP(FileNotFoundError(errno.ENOENT, "missing"))
        with mock.patch.object(self.remote, "execute") as execute:
            self.assertIsNone(self.remote.sha256(ROOT + "/missing.csv"))
        execute.assert_not_called()

    def test_permission_stat_error_propagates_instead_of_missing_interpretation(self) -> None:
        self.remote.sftp = _StatOnlySFTP(PermissionError(errno.EACCES, "denied"))
        path = ROOT + "/shared/data.csv"
        for operation in (self.remote.exists, self.remote.size, self.remote.mode, self.remote.sha256):
            with self.subTest(operation=operation.__name__), mock.patch.object(
                self.remote, "execute"
            ) as execute:
                with self.assertRaises(PermissionError):
                    operation(path)
                execute.assert_not_called()

    def test_exists_size_and_mode_return_absent_only_for_enoent(self) -> None:
        self.remote.sftp = _StatOnlySFTP(FileNotFoundError(errno.ENOENT, "missing"))
        path = ROOT + "/missing.csv"
        self.assertFalse(self.remote.exists(path))
        self.assertIsNone(self.remote.size(path))
        self.assertIsNone(self.remote.mode(path))

    def test_sha256_race_or_command_failure_is_not_interpreted_as_missing(self) -> None:
        path = ROOT + "/shared/data.csv"
        calls = 0

        def execute(command: str, timeout: int = 45) -> tuple[int, str, str]:
            nonlocal calls
            calls += 1
            if command.startswith("canonical_root="):
                return self._guard_result(path)
            return 1, "", "not found after stat"

        with mock.patch.object(self.remote, "execute", side_effect=execute):
            with self.assertRaisesRegex(RuntimeError, "command failed"):
                self.remote.sha256(path)
        self.assertEqual(calls, 2)

    def test_sha256_rejects_unsafe_or_outside_path_before_remote_access(self) -> None:
        for path in ("relative.csv", ROOT + "/../secret", ROOT + "/line\nbreak", "/tmp/not-an-artifact"):
            with self.subTest(path=path), mock.patch.object(self.remote, "execute") as execute:
                with self.assertRaisesRegex(ValueError, "configured safe POSIX root"):
                    self.remote.sha256(path)
                execute.assert_not_called()

    def test_canonical_symlink_escape_blocks_preview_before_sha256sum(self) -> None:
        path = ROOT + "/shared/link-outside.csv"
        with mock.patch.object(
            self.remote,
            "execute",
            return_value=self._guard_result(path, canonical="/outside/data.csv"),
        ) as execute:
            with self.assertRaisesRegex(RuntimeError, "canonical path validation failed"):
                self.remote.sha256(path)
        self.assertEqual(execute.call_count, 1)
        self.assertNotIn("sha256sum", execute.call_args.args[0])

    def test_preview_dangling_leaf_symlink_is_not_interpreted_as_create(self) -> None:
        path = ROOT + "/shared/dangling-link.csv"
        self.remote.sftp = _StatOnlySFTP(leaf_mode=stat.S_IFLNK | 0o777)
        with mock.patch.object(self.remote, "execute") as execute:
            with self.assertRaisesRegex(ValueError, "checksum target must not be a symlink"):
                self.remote.sha256(path)
        execute.assert_not_called()

    def test_upload_and_copy_used_by_rollback_reject_symlink_escape_before_sftp_io(self) -> None:
        source = ROOT + "/shared/source.csv"
        target = ROOT + "/shared/link-outside.csv"
        with mock.patch.object(
            self.remote,
            "execute",
            return_value=self._guard_result(target, canonical="/outside/data.csv"),
        ):
            with self.assertRaisesRegex(RuntimeError, "canonical path validation failed"):
                self.remote.copy(source, target)
            with self.assertRaisesRegex(RuntimeError, "canonical path validation failed"):
                self.remote.upload_atomic(__file__, target, None)
        self.assertEqual(self.remote.sftp.open_calls, 0)
        self.assertEqual(self.remote.sftp.put_calls, 0)

    def test_dangling_leaf_symlink_blocks_copy_upload_and_rollback_primitive(self) -> None:
        source = ROOT + "/shared/source.csv"
        dangling_target = ROOT + "/shared/dangling-link.csv"
        self.remote.sftp = _StatOnlySFTP(leaf_mode=stat.S_IFLNK | 0o777)
        with mock.patch.object(
            self.remote, "execute", return_value=self._guard_result(source)
        ):
            with self.assertRaisesRegex(ValueError, "must not be a symlink"):
                self.remote.copy(source, dangling_target)
            with self.assertRaisesRegex(ValueError, "must not be a symlink"):
                self.remote.upload_atomic(__file__, dangling_target, None)
        self.assertEqual(self.remote.sftp.open_calls, 0)
        self.assertEqual(self.remote.sftp.put_calls, 0)

    def test_backup_and_rollback_copy_create_missing_safe_parent_directories(self) -> None:
        sftp = _MemorySFTP()
        target = ROOT + "/shared/current.csv"
        backup = ROOT + "/.deployment_backups/release/nested/current.csv"
        rollback_target = ROOT + "/shared/rollback/nested/current.csv"
        sftp.files[target] = b"before"
        remote = ParamikoRemote({"remote_root": ROOT})
        remote.sftp = sftp

        def execute(command: str, timeout: int = 45) -> tuple[int, str, str]:
            if command.startswith("canonical_root="):
                return 0, f"{ROOT}\n{ROOT}\n", ""
            checksum_path = shlex.split(command)[-1]
            digest = hashlib.sha256(sftp.files[checksum_path]).hexdigest()
            return 0, f"{digest}  {checksum_path}\n", ""

        with tempfile.TemporaryDirectory() as temp:
            local = Path(temp) / "current.csv"
            local.write_bytes(b"after")
            with mock.patch.object(remote, "execute", side_effect=execute):
                remote.upload_atomic(local, target, backup)
                remote.copy(backup, rollback_target)

        self.assertEqual(sftp.files[target], b"after")
        self.assertEqual(sftp.files[backup], b"before")
        self.assertEqual(sftp.files[rollback_target], b"before")
        self.assertIn(ROOT + "/.deployment_backups/release/nested", sftp.directories)
        self.assertIn(ROOT + "/shared/rollback/nested", sftp.directories)

    def test_private_upload_sets_0600_before_writing_first_byte(self) -> None:
        sftp = _MemorySFTP()
        target = ROOT + "/shared/private.csv"
        payload = b"private-master-data"
        remote = ParamikoRemote({"remote_root": ROOT})
        remote.sftp = sftp

        def execute(command: str, timeout: int = 45) -> tuple[int, str, str]:
            checksum_path = shlex.split(command)[-1]
            digest = hashlib.sha256(sftp.files[checksum_path]).hexdigest()
            return 0, f"{digest}  {checksum_path}\n", ""

        with mock.patch.object(
            remote, "_canonical_remote_path", side_effect=lambda path, **_: path
        ), mock.patch.object(remote, "execute", side_effect=execute):
            remote.upload_bytes_atomic(payload, target, None)

        private_open = next(index for index, event in enumerate(sftp.events) if event[0] == "open" and event[2] == "wx")
        private_path = str(sftp.events[private_open][1])
        chmod = next(index for index, event in enumerate(sftp.events) if event == ("chmod", private_path, 0o600))
        first_write = next(index for index, event in enumerate(sftp.events) if event[0] == "write" and event[1] == private_path)
        self.assertLess(private_open, chmod)
        self.assertLess(chmod, first_write)
        self.assertEqual(sftp.files[target], payload)
        self.assertEqual(sftp.modes[target], 0o600)

    def test_size_and_mode_stat_error_handling_and_canonical_guard(self) -> None:
        path = ROOT + "/shared/data.csv"
        with mock.patch.object(
            self.remote, "execute", return_value=self._guard_result(path)
        ):
            self.assertEqual(self.remote.size(path), 123)
            self.assertEqual(self.remote.mode(path), 0o640)
        self.assertEqual(self.remote.sftp.stat_calls, 2)


if __name__ == "__main__":
    unittest.main()
