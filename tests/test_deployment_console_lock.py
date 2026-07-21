from __future__ import annotations

import unittest
from types import SimpleNamespace

from services.deploy.console_backend import ParamikoRemote


class _FakeLockFile:
    def __init__(self, sftp: "_FakeSFTP", path: str) -> None:
        self.sftp = sftp
        self.path = path

    def __enter__(self) -> "_FakeLockFile":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def write(self, value: bytes) -> None:
        if not isinstance(value, bytes):
            raise TypeError("SFTP lock content must be bytes")
        if self.sftp.fail_write:
            raise OSError("simulated write failure")
        self.sftp.files[self.path] = value

    def flush(self) -> None:
        self.sftp.flush_count += 1
        if self.sftp.fail_flush:
            raise OSError("simulated flush failure")


class _FakeSFTP:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.fail_write = False
        self.fail_flush = False
        self.flush_count = 0
        self.remove_calls: list[str] = []

    def open(self, path: str, mode: str) -> _FakeLockFile:
        if mode != "wx":
            raise AssertionError(mode)
        if path in self.files:
            raise FileExistsError(path)
        self.files[path] = b""
        return _FakeLockFile(self, path)

    def stat(self, path: str) -> SimpleNamespace:
        if path not in self.files:
            raise FileNotFoundError(path)
        return SimpleNamespace(st_size=len(self.files[path]))

    def lstat(self, path: str) -> SimpleNamespace:
        stat = self.stat(path)
        return SimpleNamespace(st_size=stat.st_size, st_mode=0o100600)

    def remove(self, path: str) -> None:
        self.remove_calls.append(path)
        if path not in self.files:
            raise FileNotFoundError(path)
        del self.files[path]


class DeploymentLockTests(unittest.TestCase):
    def setUp(self) -> None:
        self.lock_path = "/home/csda/AI_Routing/.deployment-console.lock"
        self.sftp = _FakeSFTP()
        self.remote = ParamikoRemote({"remote_root": "/home/csda/AI_Routing"})
        self.remote.sftp = self.sftp
        self.remote.mkdirs = lambda _path: None  # type: ignore[method-assign]
        self.remote._canonical_remote_path = (  # type: ignore[method-assign]
            lambda path, *, target_must_exist: str(path)
        )

    def test_lock_writes_bytes_flushes_and_removes_only_after_use(self) -> None:
        with self.remote.deployment_lock(
            "/home/csda/AI_Routing", "development-runtime-v1"
        ):
            self.assertEqual(
                self.sftp.files[self.lock_path], b"development-runtime-v1"
            )
            self.assertEqual(self.sftp.flush_count, 1)
            self.assertEqual(self.sftp.remove_calls, [])
        self.assertNotIn(self.lock_path, self.sftp.files)
        self.assertEqual(self.sftp.remove_calls, [self.lock_path])

    def test_write_failure_cleans_up_lock_created_by_this_session(self) -> None:
        self.sftp.fail_write = True
        with self.assertRaisesRegex(RuntimeError, "initialize"):
            with self.remote.deployment_lock(
                "/home/csda/AI_Routing", "development-runtime-v1"
            ):
                self.fail("lock body must not run")
        self.assertNotIn(self.lock_path, self.sftp.files)
        self.assertEqual(self.sftp.remove_calls, [self.lock_path])

    def test_flush_failure_cleans_up_lock_created_by_this_session(self) -> None:
        self.sftp.fail_flush = True
        with self.assertRaisesRegex(RuntimeError, "initialize"):
            with self.remote.deployment_lock(
                "/home/csda/AI_Routing", "development-runtime-v1"
            ):
                self.fail("lock body must not run")
        self.assertNotIn(self.lock_path, self.sftp.files)
        self.assertEqual(self.sftp.remove_calls, [self.lock_path])

    def test_existing_lock_open_failure_never_deletes_foreign_lock(self) -> None:
        self.sftp.files[self.lock_path] = b"another-deployment"
        with self.assertRaisesRegex(RuntimeError, "Another deployment"):
            with self.remote.deployment_lock(
                "/home/csda/AI_Routing", "development-runtime-v1"
            ):
                self.fail("lock body must not run")
        self.assertEqual(self.sftp.files[self.lock_path], b"another-deployment")
        self.assertEqual(self.sftp.remove_calls, [])


if __name__ == "__main__":
    unittest.main()
