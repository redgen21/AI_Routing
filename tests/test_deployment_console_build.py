from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from services.deploy import console_backend


class RuntimeArtifactBuildTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.deployment_root = Path(self.temp.name) / "deployment"
        self.root_patch = mock.patch.object(
            console_backend, "DEPLOYMENT_ROOT", self.deployment_root
        )
        self.root_patch.start()

    def tearDown(self) -> None:
        self.root_patch.stop()
        self.temp.cleanup()

    @staticmethod
    def _completed(argv: list[str], stdout: str = "", stderr: str = "", code: int = 0):
        return subprocess.CompletedProcess(argv, code, stdout, stderr)

    @staticmethod
    def _runtime_manifest(environment: str, *, dirty: bool) -> dict[str, object]:
        return {
            "artifact_type": "server-runtime",
            "target_environment": environment,
            "target_root": f"/home/csda/AI_Routing/{environment}",
            "source_dirty": dirty,
            "source_mode": (
                "immutable-git-archive" if environment == "production" else "worktree"
            ),
            "promotable": environment == "production",
        }

    def test_invalid_version_is_rejected_before_any_subprocess(self) -> None:
        with mock.patch.object(console_backend.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "version"):
                console_backend.preview_runtime_build(
                    environment="development", version="bad version;whoami"
                )
        run.assert_not_called()

    def test_preview_uses_only_fixed_git_probes_and_hides_changed_names(self) -> None:
        results = [
            self._completed([], "a" * 40 + "\n"),
            self._completed([], " M secret-name.py\n?? another-private-name.txt\n"),
        ]
        with mock.patch.object(console_backend.subprocess, "run", side_effect=results) as run:
            preview = console_backend.preview_runtime_build(
                environment="development", version="v1"
            )
        self.assertTrue(preview["source_dirty"])
        self.assertEqual(preview["source_change_count"], 2)
        self.assertNotIn("secret-name", repr(preview))
        self.assertEqual(run.call_args_list[0].args[0], ["git", "rev-parse", "HEAD"])
        self.assertEqual(
            run.call_args_list[1].args[0],
            ["git", "status", "--porcelain", "--untracked-files=all"],
        )
        for call in run.call_args_list:
            self.assertIs(call.kwargs["shell"], False)

    def test_process_output_decodes_utf8_windows_locale_utf16_and_newlines(self) -> None:
        self.assertEqual(
            console_backend._decode_process_output("한글\r\nnext\rline".encode("utf-8")),
            "한글\nnext\nline",
        )
        with mock.patch.object(
            console_backend.locale, "getpreferredencoding", return_value="cp949"
        ):
            self.assertEqual(
                console_backend._decode_process_output("한글 오류\r\n".encode("cp949")),
                "한글 오류\n",
            )
        self.assertEqual(
            console_backend._decode_process_output("PowerShell 한글\r\n".encode("utf-16")),
            "PowerShell 한글\n",
        )

    def test_local_process_captures_bytes_with_forced_python_utf8_environment(self) -> None:
        completed = subprocess.CompletedProcess(["tool"], 0, b"ok", b"")
        with mock.patch.object(
            console_backend.subprocess, "run", return_value=completed
        ) as run:
            result = console_backend._run_local_process(["tool"], timeout=9)
        self.assertIs(result, completed)
        self.assertIs(run.call_args.kwargs["text"], False)
        self.assertEqual(run.call_args.kwargs["env"]["PYTHONUTF8"], "1")
        self.assertEqual(run.call_args.kwargs["env"]["PYTHONIOENCODING"], "utf-8")
        self.assertNotIn("encoding", run.call_args.kwargs)

    def test_dirty_development_requires_opt_in_and_adds_only_dirty_switch(self) -> None:
        with mock.patch.object(
            console_backend, "_git_source_state", return_value=("a" * 40, 2)
        ):
            with self.assertRaisesRegex(PermissionError, "explicit"):
                console_backend.build_runtime_artifact(
                    environment="development", version="v1"
                )

        paths = console_backend._runtime_build_paths("development", "v1")

        def fake_run(argv: list[str], *, timeout: int):
            paths["staging"].mkdir(parents=True)
            paths["manifest"].write_text(
                json.dumps(self._runtime_manifest("development", dirty=True)),
                encoding="utf-8",
            )
            paths["archive"].write_bytes(b"zip")
            return self._completed(argv)

        inspection = SimpleNamespace(
            version="v1",
            archive_sha256="b" * 64,
            manifest={
                "source_revision": "a" * 40,
                "source_dirty": True,
                "source_mode": "worktree",
            },
        )
        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 2)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="C:/pwsh.exe"),
            mock.patch.object(console_backend, "_run_local_process", side_effect=fake_run) as run,
            mock.patch.object(console_backend, "inspect_artifact", return_value=inspection),
        ):
            receipt = console_backend.build_runtime_artifact(
                environment="development", version="v1", allow_dirty_source=True
            )
        argv = run.call_args.args[0]
        self.assertEqual(
            argv,
            [
                "C:/pwsh.exe",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(console_backend._BUILD_SCRIPT),
                "-Version",
                "v1",
                "-OutputDir",
                "deployment",
                "-Environment",
                "development",
                "-AllowDirtySource",
            ],
        )
        self.assertEqual(run.call_args.kwargs, {"timeout": 600})
        self.assertEqual(receipt["status"], "built")
        self.assertEqual(receipt["source_mode"], "worktree")
        self.assertEqual(
            [entry.version for entry in console_backend.list_artifacts(
                environment="development", kind="runtime"
            )],
            ["v1"],
        )

    def test_production_dirty_is_blocked_and_clean_never_gets_bypass(self) -> None:
        with mock.patch.object(
            console_backend, "_git_source_state", return_value=("a" * 40, 1)
        ):
            with self.assertRaisesRegex(PermissionError, "clean"):
                console_backend.build_runtime_artifact(
                    environment="production", version="v1"
                )
            with self.assertRaisesRegex(PermissionError, "cannot bypass"):
                console_backend.build_runtime_artifact(
                    environment="production", version="v2", allow_dirty_source=True
                )

        paths = console_backend._runtime_build_paths("production", "v3")

        def fake_run(argv: list[str], *, timeout: int):
            paths["staging"].mkdir(parents=True)
            paths["manifest"].write_text(
                json.dumps(self._runtime_manifest("production", dirty=False)),
                encoding="utf-8",
            )
            paths["archive"].write_bytes(b"zip")
            return self._completed(argv)

        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 0)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="pwsh"),
            mock.patch.object(console_backend, "_run_local_process", side_effect=fake_run) as run,
            mock.patch.object(
                console_backend,
                "inspect_artifact",
                return_value=SimpleNamespace(
                    version="v3",
                    archive_sha256="c" * 64,
                    manifest={
                        "source_revision": "a" * 40,
                        "source_dirty": False,
                        "source_mode": "immutable-git-archive",
                    },
                ),
            ),
        ):
            console_backend.build_runtime_artifact(
                environment="production", version="v3"
            )
        self.assertNotIn("-AllowDirtySource", run.call_args.args[0])

    def test_success_requires_manifest_to_match_prebuild_source_state(self) -> None:
        paths = console_backend._runtime_build_paths("development", "mismatch")

        def fake_run(argv: list[str], *, timeout: int):
            paths["staging"].mkdir(parents=True)
            paths["manifest"].write_text("{}", encoding="utf-8")
            paths["archive"].write_bytes(b"zip")
            return self._completed(argv)

        inspection = SimpleNamespace(
            version="mismatch",
            archive_sha256="d" * 64,
            manifest={
                "source_revision": "b" * 40,
                "source_dirty": False,
                "source_mode": "worktree",
            },
        )
        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 0)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="pwsh"),
            mock.patch.object(console_backend, "_run_local_process", side_effect=fake_run),
            mock.patch.object(console_backend, "inspect_artifact", return_value=inspection),
        ):
            with self.assertRaisesRegex(RuntimeError, "source state"):
                console_backend.build_runtime_artifact(
                    environment="development", version="mismatch"
                )
        self.assertEqual(
            console_backend.list_artifacts(environment="development", kind="runtime"),
            [],
        )
        self.assertFalse(paths["staging"].exists())
        self.assertFalse(paths["archive"].exists())
        self.assertTrue((self.deployment_root / "development" / "_failed").is_dir())

    def test_existing_output_and_build_lock_block_before_process(self) -> None:
        paths = console_backend._runtime_build_paths("development", "existing")
        paths["staging"].mkdir(parents=True)
        with mock.patch.object(console_backend, "_run_local_process") as run:
            with self.assertRaisesRegex(FileExistsError, "already exists"):
                console_backend.build_runtime_artifact(
                    environment="development", version="existing"
                )
        run.assert_not_called()

        self.assertTrue(console_backend._BUILD_LOCK.acquire(blocking=False))
        try:
            with self.assertRaisesRegex(RuntimeError, "already in progress"):
                console_backend.build_runtime_artifact(
                    environment="development", version="other"
                )
        finally:
            console_backend._BUILD_LOCK.release()

    def test_timeout_output_is_redacted_and_truncated(self) -> None:
        secret = "top-secret-value"
        failure = subprocess.TimeoutExpired(
            cmd=["pwsh"],
            timeout=600,
            output=(f"token={secret} 한글\r\n" + "x" * 5000).encode("utf-8"),
        )
        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 0)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="pwsh"),
            mock.patch.object(console_backend, "_run_local_process", side_effect=failure),
        ):
            with self.assertRaises(TimeoutError) as raised:
                console_backend.build_runtime_artifact(
                    environment="development", version="timeout"
                )
        message = str(raised.exception)
        self.assertNotIn(secret, message)
        self.assertIn("[REDACTED]", message)
        self.assertIn("한글", message)
        self.assertLess(len(message), 4300)

    def test_nonzero_build_outputs_are_quarantined_before_original_error(self) -> None:
        paths = console_backend._runtime_build_paths("development", "code1")

        def fail_with_outputs(argv: list[str], *, timeout: int):
            paths["staging"].mkdir(parents=True)
            paths["manifest"].write_text("{}", encoding="utf-8")
            paths["archive"].write_bytes(b"zip")
            return self._completed(argv, stderr="build failed", code=1)

        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 0)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="pwsh"),
            mock.patch.object(
                console_backend, "_run_local_process", side_effect=fail_with_outputs
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "exit code 1"):
                console_backend.build_runtime_artifact(
                    environment="development", version="code1"
                )
        self.assertFalse(paths["staging"].exists())
        self.assertFalse(paths["archive"].exists())
        self.assertEqual(
            console_backend.list_artifacts(environment="development", kind="runtime"), []
        )

    def test_timeout_build_outputs_are_quarantined_before_original_timeout(self) -> None:
        paths = console_backend._runtime_build_paths("development", "timed-outputs")

        def timeout_with_outputs(argv: list[str], *, timeout: int):
            paths["staging"].mkdir(parents=True)
            paths["manifest"].write_text("{}", encoding="utf-8")
            paths["archive"].write_bytes(b"zip")
            raise subprocess.TimeoutExpired(cmd=argv, timeout=timeout, output="timed out")

        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 0)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="pwsh"),
            mock.patch.object(
                console_backend, "_run_local_process", side_effect=timeout_with_outputs
            ),
        ):
            with self.assertRaisesRegex(TimeoutError, "timed out"):
                console_backend.build_runtime_artifact(
                    environment="development", version="timed-outputs"
                )
        self.assertFalse(paths["staging"].exists())
        self.assertFalse(paths["archive"].exists())
        self.assertEqual(
            console_backend.list_artifacts(environment="development", kind="runtime"), []
        )

    def test_quarantine_fallback_hides_manifest_and_archive_names(self) -> None:
        paths = console_backend._runtime_build_paths("development", "fallback")
        paths["staging"].mkdir(parents=True)
        paths["manifest"].write_text("{}", encoding="utf-8")
        paths["archive"].write_bytes(b"zip")
        with mock.patch.object(Path, "mkdir", side_effect=OSError("quarantine unavailable")):
            console_backend._quarantine_failed_runtime_build(
                environment="development", paths=paths
            )
        self.assertFalse(paths["manifest"].exists())
        self.assertFalse(paths["archive"].exists())
        self.assertEqual(
            console_backend.list_artifacts(environment="development", kind="runtime"),
            [],
        )
        self.assertEqual(len(list(paths["staging"].glob(".failed-deploy_manifest-*.json"))), 1)
        self.assertEqual(
            len(list(paths["archive"].parent.glob(".failed-*-ai-routing-runtime-*.zip"))),
            1,
        )

    def test_package_script_refuses_to_overwrite_outputs(self) -> None:
        source = console_backend._BUILD_SCRIPT.read_text(encoding="utf-8")
        collision_block = source[source.index("if (Test-Path $FinalStagingDir)") :]
        self.assertIn("throw", collision_block)
        self.assertNotIn("Remove-Item -LiteralPath $StagingDir", collision_block)
        self.assertNotIn("Remove-Item -LiteralPath $ZipPath", collision_block)

    def test_runtime_listing_hides_legacy_policy_without_hashing_files(self) -> None:
        root = self.deployment_root / "development"
        valid = root / "current"
        legacy_missing = root / "2026.07.19-server-allowlist"
        legacy_conflicting = root / "2026.07.18-refactor"
        for staging in (valid, legacy_missing, legacy_conflicting):
            staging.mkdir(parents=True)

        (valid / "deploy_manifest.json").write_text(
            json.dumps(self._runtime_manifest("development", dirty=True)),
            encoding="utf-8",
        )
        legacy = self._runtime_manifest("development", dirty=True)
        legacy.pop("source_mode")
        legacy.pop("promotable")
        (legacy_missing / "deploy_manifest.json").write_text(
            json.dumps(legacy), encoding="utf-8"
        )
        conflicting = self._runtime_manifest("development", dirty=False)
        conflicting["promotable"] = True
        (legacy_conflicting / "deploy_manifest.json").write_text(
            json.dumps(conflicting), encoding="utf-8"
        )

        with mock.patch.object(
            console_backend, "_sha256_file", side_effect=AssertionError("hashing is not listing")
        ):
            entries = console_backend.list_artifacts(
                environment="development", kind="runtime"
            )
        self.assertEqual([entry.version for entry in entries], ["current"])

    def test_non_runtime_listing_does_not_parse_or_hash_large_manifest(self) -> None:
        staging = self.deployment_root / "server_data" / "large-v1"
        staging.mkdir(parents=True)
        (staging / "manifest.json").write_text("{}", encoding="utf-8")
        with (
            mock.patch.object(
                console_backend, "_read_json", side_effect=AssertionError("must stay lazy")
            ),
            mock.patch.object(
                console_backend, "_sha256_file", side_effect=AssertionError("must stay lazy")
            ),
        ):
            entries = console_backend.list_artifacts(
                environment="development", kind="server-data"
            )
        self.assertEqual([entry.version for entry in entries], ["large-v1"])

    def test_production_package_uses_immutable_snapshot_and_hidden_publish_session(self) -> None:
        source = console_backend._BUILD_SCRIPT.read_text(encoding="utf-8")
        self.assertIn('& git @GitArchiveArgs', source)
        self.assertIn('"archive",', source)
        self.assertIn('$SourcePath = Join-Path $SourceRoot $SourceRelativePath', source)
        self.assertIn('$SourceDir = Join-Path $SourceRoot $DirectoryName', source)
        self.assertIn('source_mode = $SourceMode', source)
        self.assertIn('promotable = [bool]($IsProduction -and -not $SourceDirty)', source)
        self.assertIn('Join-Path $EnvironmentOutputRoot "_building"', source)
        self.assertLess(
            source.index('Move-PublishPathWithRetry -Kind "File"'),
            source.index('Move-PublishPathWithRetry -Kind "Directory"'),
        )
        self.assertIn('[int]$MaxAttempts = 8', source)
        self.assertIn('Start-Sleep -Milliseconds ($DelayMilliseconds * $Attempt)', source)
        self.assertIn('function Get-FileSha256WithRetry', source)
        self.assertIn('$BuiltZipSha256 = Get-FileSha256WithRetry -Path $ZipPath', source)
        self.assertIn('finally {', source)
        self.assertIn('^ai-routing-runtime-snapshot-[0-9a-f]{32}$', source)
        self.assertIn('^runtime-[0-9a-f]{32}$', source)

    def test_package_root_normalization_is_provider_free_and_unc_safe(self) -> None:
        builders = (
            console_backend._BUILD_SCRIPT,
            console_backend.PROJECT_ROOT / "services/deploy/build_admin_tools_package.ps1",
            console_backend.PROJECT_ROOT / "services/deploy/build_server_data_package.ps1",
        )
        for builder in builders:
            source = builder.read_text(encoding="utf-8")
            self.assertIn("function ConvertTo-NativeFileSystemPath", source)
            self.assertIn("GetUnresolvedProviderPathFromPSPath", source)
            self.assertIn('$Provider.Name -ne "FileSystem"', source)
            self.assertIn("[System.IO.Path]::GetFullPath", source)
            self.assertNotIn("$Root = Resolve-Path", source)
            self.assertNotIn("$Root = (Resolve-Path", source)
            self.assertIn("Provider-qualified paths are not valid", source)

        if os.name != "nt":
            self.skipTest("PowerShell filesystem provider regression is Windows-specific")
        powershell = shutil.which("powershell") or shutil.which("pwsh")
        if not powershell:
            self.skipTest("PowerShell is unavailable")
        unicode_dir = Path(self.temp.name) / "한글 경로"
        unicode_dir.mkdir()
        command = (
            "$tokens=$null;$errors=$null;"
            "$ast=[System.Management.Automation.Language.Parser]::ParseFile($env:CODEX_TEST_SCRIPT,"
            "[ref]$tokens,[ref]$errors);"
            "$fn=$ast.Find({param($n) $n -is "
            "[System.Management.Automation.Language.FunctionDefinitionAst] -and "
            "$n.Name -eq 'ConvertTo-NativeFileSystemPath'},$true);"
            ". ([scriptblock]::Create($fn.Extent.Text));"
            "[Console]::OutputEncoding=New-Object System.Text.UTF8Encoding($false);"
            "$value=ConvertTo-NativeFileSystemPath -Path $env:CODEX_TEST_PATH -MustExist;"
            "[Console]::Write($value)"
        )
        provider_path = "Microsoft.PowerShell.Core\\FileSystem::" + str(unicode_dir)
        for builder in builders:
            child_env = os.environ.copy()
            child_env["CODEX_TEST_SCRIPT"] = str(builder)
            child_env["CODEX_TEST_PATH"] = provider_path
            result = subprocess.run(
                [powershell, "-NoProfile", "-NonInteractive", "-Command", command],
                capture_output=True,
                text=False,
                env=child_env,
                shell=False,
                check=False,
                timeout=20,
            )
            self.assertEqual(
                result.returncode,
                0,
                console_backend._decode_process_output(result.stderr),
            )
            normalized = console_backend._decode_process_output(result.stdout)
            self.assertEqual(Path(normalized), unicode_dir.resolve())
            self.assertNotIn("::", normalized)

            unc_env = child_env.copy()
            unc_env["CODEX_TEST_PATH"] = (
                "Microsoft.PowerShell.Core\\FileSystem::"
                "\\\\server.example\\share\\한글 경로"
            )
            unc_command = command.replace(" -MustExist", "")
            unc_result = subprocess.run(
                [powershell, "-NoProfile", "-NonInteractive", "-Command", unc_command],
                capture_output=True,
                text=False,
                env=unc_env,
                shell=False,
                check=False,
                timeout=20,
            )
            self.assertEqual(
                unc_result.returncode,
                0,
                console_backend._decode_process_output(unc_result.stderr),
            )
            normalized_unc = console_backend._decode_process_output(unc_result.stdout)
            self.assertTrue(normalized_unc.startswith("\\\\server.example\\share\\"))
            self.assertIn("한글 경로", normalized_unc)
            self.assertNotIn("::", normalized_unc)

class AdminToolsArtifactBuildTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.deployment_root = Path(self.temp.name) / "deployment"
        self.root_patch = mock.patch.object(
            console_backend, "DEPLOYMENT_ROOT", self.deployment_root
        )
        self.root_patch.start()

    def tearDown(self) -> None:
        self.root_patch.stop()
        self.temp.cleanup()

    @staticmethod
    def _completed(argv: list[str], stdout: str = "", stderr: str = "", code: int = 0):
        return subprocess.CompletedProcess(argv, code, stdout, stderr)

    @staticmethod
    def _inspection(version: str, revision: str, *, dirty: bool) -> SimpleNamespace:
        return SimpleNamespace(
            version=version,
            manifest={
                "artifact_type": "db-admin-tools",
                "source_revision": revision,
                "source_dirty": dirty,
                "promotable": not dirty,
            },
        )

    def test_preview_is_local_nonsecret_and_dirty_state_is_nonpromotable(self) -> None:
        with mock.patch.object(
            console_backend, "_git_source_state", return_value=("a" * 40, 3)
        ):
            preview = console_backend.preview_admin_tools_build(version="admin-v1")
        self.assertEqual(preview["status"], "preview")
        self.assertEqual(preview["kind"], "admin-tools")
        self.assertTrue(preview["source_dirty"])
        self.assertFalse(preview["promotable"])
        self.assertTrue(preview["requires_dirty_approval"])

    def test_invalid_version_and_collision_block_before_build_process(self) -> None:
        with mock.patch.object(console_backend, "_run_local_process") as run:
            with self.assertRaisesRegex(ValueError, "version"):
                console_backend.build_admin_tools_artifact(version="bad version;whoami")
        run.assert_not_called()

        paths = console_backend._admin_tools_build_paths("existing")
        paths["staging"].mkdir(parents=True)
        with mock.patch.object(console_backend, "_run_local_process") as run:
            with self.assertRaisesRegex(FileExistsError, "already exists"):
                console_backend.build_admin_tools_artifact(version="existing")
        run.assert_not_called()

    def test_dirty_build_requires_approval_and_returns_verification_receipt(self) -> None:
        with mock.patch.object(
            console_backend, "_git_source_state", return_value=("a" * 40, 1)
        ):
            with self.assertRaisesRegex(PermissionError, "explicit"):
                console_backend.build_admin_tools_artifact(version="dirty-v1")

        paths = console_backend._admin_tools_build_paths("dirty-v1")

        def fake_run(argv: list[str], *, timeout: int):
            paths["staging"].mkdir(parents=True)
            paths["manifest"].write_text("{}", encoding="utf-8")
            paths["archive"].write_bytes(b"admin-zip")
            return self._completed(argv)

        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 1)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="C:/pwsh.exe"),
            mock.patch.object(console_backend, "_run_local_process", side_effect=fake_run) as run,
            mock.patch.object(
                console_backend,
                "inspect_artifact",
                return_value=self._inspection("dirty-v1", "a" * 40, dirty=True),
            ),
            mock.patch.object(console_backend, "_sha256_file", return_value="d" * 64),
        ):
            receipt = console_backend.build_admin_tools_artifact(
                version="dirty-v1", allow_dirty_source=True
            )
        self.assertEqual(
            run.call_args.args[0],
            [
                "C:/pwsh.exe",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(console_backend._ADMIN_TOOLS_BUILD_SCRIPT),
                "-Version",
                "dirty-v1",
                "-OutputDir",
                "deployment",
                "-AllowDirtySource",
            ],
        )
        self.assertEqual(run.call_args.kwargs, {"timeout": 600})
        self.assertEqual(receipt["status"], "built")
        self.assertEqual(receipt["kind"], "admin-tools")
        self.assertTrue(receipt["source_dirty"])
        self.assertFalse(receipt["promotable"])
        self.assertEqual(receipt["archive_sha256"], "d" * 64)

    def test_clean_build_is_promotable_and_manifest_mismatch_is_quarantined(self) -> None:
        paths = console_backend._admin_tools_build_paths("clean-v1")

        def fake_run(argv: list[str], *, timeout: int):
            paths["staging"].mkdir(parents=True)
            paths["manifest"].write_text("{}", encoding="utf-8")
            paths["archive"].write_bytes(b"admin-zip")
            return self._completed(argv)

        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 0)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="pwsh"),
            mock.patch.object(console_backend, "_run_local_process", side_effect=fake_run) as run,
            mock.patch.object(
                console_backend,
                "inspect_artifact",
                return_value=self._inspection("clean-v1", "a" * 40, dirty=False),
            ),
            mock.patch.object(console_backend, "_sha256_file", return_value="c" * 64),
        ):
            receipt = console_backend.build_admin_tools_artifact(version="clean-v1")
        self.assertNotIn("-AllowDirtySource", run.call_args.args[0])
        self.assertFalse(receipt["source_dirty"])
        self.assertTrue(receipt["promotable"])

        mismatch = console_backend._admin_tools_build_paths("mismatch")

        def mismatch_run(argv: list[str], *, timeout: int):
            mismatch["staging"].mkdir(parents=True)
            mismatch["manifest"].write_text("{}", encoding="utf-8")
            mismatch["archive"].write_bytes(b"admin-zip")
            return self._completed(argv)

        with (
            mock.patch.object(
                console_backend, "_git_source_state", return_value=("a" * 40, 0)
            ),
            mock.patch.object(console_backend.shutil, "which", return_value="pwsh"),
            mock.patch.object(console_backend, "_run_local_process", side_effect=mismatch_run),
            mock.patch.object(
                console_backend,
                "inspect_artifact",
                return_value=self._inspection("mismatch", "b" * 40, dirty=False),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "source state"):
                console_backend.build_admin_tools_artifact(version="mismatch")
        self.assertFalse(mismatch["staging"].exists())
        self.assertFalse(mismatch["archive"].exists())
        self.assertTrue((self.deployment_root / "admin_tools" / "_failed").is_dir())


if __name__ == "__main__":
    unittest.main()
