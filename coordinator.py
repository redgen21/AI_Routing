#!/usr/bin/env python3
"""coordinator.py — Claude ↔ Codex 자동 피드백 루프 조율기

사용법:
  python coordinator.py                    # 기본 3라운드 실행
  python coordinator.py --rounds 5        # 5라운드
  python coordinator.py --mode codex-only # Codex 구현만 (Claude 검토 생략)
  python coordinator.py --mode claude-only # Claude 검토만 (Codex 구현 생략)
  python coordinator.py --mode review     # 테스트 + Claude 검토만
  python coordinator.py --dry-run         # 실제 API 호출 없이 흐름 확인

환경 변수:
  ANTHROPIC_API_KEY   Claude API 키
  OPENAI_API_KEY      Codex(OpenAI) API 키
  COORDINATOR_CLAUDE_MODEL  (선택) 기본값: claude-sonnet-4-6
  COORDINATOR_CODEX_MODEL   (선택) 기본값: gpt-4o
  COORDINATOR_WORKER_PYTHON (선택) compile/test/benchmark 실행용 python
"""

import os
import sys
import io
import json
import re
import subprocess
import datetime
import argparse
import textwrap
from pathlib import Path

# Windows 콘솔 UTF-8 출력 강제
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ─────────────────────────────────────────────
# 설정
# ─────────────────────────────────────────────

REQUIRED_WORKER_MODULES = ("pandas", "numpy", "scipy", "ortools")


def _has_required_worker_modules(python_bin: str) -> bool:
    check_code = (
        "import importlib.util; "
        f"mods={list(REQUIRED_WORKER_MODULES)!r}; "
        "print(all(importlib.util.find_spec(m) is not None for m in mods))"
    )
    try:
        result = subprocess.run(
            [python_bin, "-c", check_code],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=20,
            encoding="utf-8",
            errors="replace",
        )
        return result.returncode == 0 and result.stdout.strip() == "True"
    except Exception:
        return False


def _detect_worker_python_bin() -> str:
    override = os.environ.get("COORDINATOR_WORKER_PYTHON", "").strip()
    if override:
        return override

    candidates: list[str] = []
    try:
        result = subprocess.run(
            ["where.exe", "python"],
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=10,
            encoding="utf-8",
            errors="replace",
        )
        for line in result.stdout.splitlines():
            candidate = line.strip()
            if candidate and candidate not in candidates:
                candidates.append(candidate)
    except Exception:
        pass

    if sys.executable and sys.executable not in candidates:
        candidates.append(sys.executable)

    for candidate in candidates:
        if _has_required_worker_modules(candidate):
            return candidate
    return sys.executable

PROJECT_ROOT   = Path(__file__).parent
CODEX_LOG      = PROJECT_ROOT / "docs" / "UPDATED_BY_CODEX.md"
CLAUDE_LOG     = PROJECT_ROOT / "docs" / "UPDATED_BY_CLAUDE.md"
DESIGN_DOC     = PROJECT_ROOT / "docs" / "algorithm_design_csi_sits.md"

# coordinator 본체는 현재 인터프리터에서 API를 호출하고,
# worker python은 compile/test/benchmark 전용으로 분리한다.
PYTHON_BIN     = _detect_worker_python_bin()

CLAUDE_MODEL   = os.environ.get("COORDINATOR_CLAUDE_MODEL", "claude-sonnet-4-6")
CODEX_MODEL    = os.environ.get("COORDINATOR_CODEX_MODEL",  "gpt-4o")

# 라운드 성공 판정: Claude 피드백에 FAIL이 없고 PASS가 있을 때
SUCCESS_CONDITION = lambda feedback: "[PASS]" in feedback and "[FAIL]" not in feedback


# ─────────────────────────────────────────────
# 유틸
# ─────────────────────────────────────────────

def now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def read_file(path: Path, max_chars: int = 0) -> str:
    if not path.exists():
        return f"(파일 없음: {path})"
    text = path.read_text(encoding="utf-8")
    if max_chars and len(text) > max_chars:
        return text[-max_chars:]   # 마지막 N자 (최신 내용)
    return text


def append_log(path: Path, section_title: str, content: str):
    """로그 파일에 타임스탬프 섹션 추가"""
    path.parent.mkdir(parents=True, exist_ok=True)
    header = f"\n## {now_str()} — {section_title}\n\n"
    with open(path, "a", encoding="utf-8") as f:
        f.write(header + content.strip() + "\n")
    print(f"  → {path.name} 기록 완료")


def run_command(cmd: list[str], timeout: int = 180) -> tuple[bool, str]:
    """명령 실행, (성공여부, 출력) 반환"""
    try:
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
            errors="replace",
        )
        output = (result.stdout + result.stderr).strip()
        return result.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, f"[TIMEOUT] {timeout}초 초과"
    except Exception as e:
        return False, f"[ERROR] {e}"


# ─────────────────────────────────────────────
# Codex 호출
# ─────────────────────────────────────────────

def _build_codex_prompt(round_num: int) -> str:
    """Codex에게 전달할 메시지 구성"""
    design   = read_file(DESIGN_DOC, max_chars=8000)
    feedback = read_file(CLAUDE_LOG, max_chars=4000)

    # 현재 존재하는 구현 파일 상태 확인
    impl_files = [
        "smart_routing/production_assign_atlanta_csi.py",
        "smart_routing/production_assign_atlanta_sits.py",
        "sr_test_csi_sits_unit.py",
        "sr_benchmark_csi_sits_vs_vrp.py",
    ]
    existing = []
    for f in impl_files:
        p = PROJECT_ROOT / f
        status = f"{'존재' if p.exists() else '없음'}"
        size   = f"({p.stat().st_size:,}bytes)" if p.exists() else ""
        existing.append(f"  - {f}: {status} {size}")

    return textwrap.dedent(f"""\
    ## 역할
    당신은 Python 라우팅 최적화 엔지니어입니다.
    Claude의 피드백을 반영하여 CSI/SITS 알고리즘을 구현하거나 개선하세요.

    ## 현재 라운드
    Round {round_num}

    ## 현재 파일 존재 여부
    {chr(10).join(existing)}

    ## 상세 설계 문서 (전체 구현 기준)
    {design}

    ## Claude 최신 피드백 (반드시 반영)
    {feedback}

    ## 출력 형식 (반드시 준수)
    변경할 각 파일을 다음 형식으로 출력하세요:

    <file path="상대/경로/파일명.py">
    # 파일 전체 내용
    </file>

    모든 파일 출력 후, 다음 형식으로 요약 작성:

    <summary>
    - 변경 파일 목록
    - 주요 구현 내용
    - 검증 명령어
    </summary>
    """)


def call_codex(round_num: int, dry_run: bool = False) -> tuple[list[dict], str]:
    """
    Codex(OpenAI) 호출 → (파일목록, 요약) 반환
    파일목록: [{"path": "...", "content": "..."}]
    """
    if dry_run:
        print("  [DRY-RUN] Codex 호출 생략")
        return [], "dry-run: 변경 없음"

    try:
        import openai
    except ImportError:
        print("  [ERROR] openai 패키지가 설치되지 않았습니다")
        return [], "openai 패키지 없음"

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("  [ERROR] OPENAI_API_KEY 환경 변수가 설정되지 않았습니다")
        return [], "API 키 없음"

    client = openai.OpenAI(api_key=api_key)
    prompt = _build_codex_prompt(round_num)

    print(f"  모델: {CODEX_MODEL}")
    try:
        response = client.chat.completions.create(
            model=CODEX_MODEL,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert Python engineer. "
                        "Output complete file contents in the specified XML format. "
                        "Do not truncate files — output the entire content of each file."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=16000,
            temperature=0.1,
        )
    except Exception as e:
        err = f"[ERROR] Codex API call failed: {type(e).__name__}: {e}"
        print(f"  {err}")
        return [], err

    raw = response.choices[0].message.content
    files   = _parse_file_blocks(raw)
    summary = _parse_summary_block(raw)
    return files, summary


def _parse_file_blocks(text: str) -> list[dict]:
    """<file path="...">...</file> 블록 파싱"""
    pattern = re.compile(
        r'<file\s+path=["\']([^"\']+)["\']\s*>\n?(.*?)\n?</file>',
        re.DOTALL,
    )
    results = []
    for m in pattern.finditer(text):
        results.append({"path": m.group(1).strip(), "content": m.group(2)})
    return results


def _parse_summary_block(text: str) -> str:
    m = re.search(r"<summary>(.*?)</summary>", text, re.DOTALL)
    if m:
        return m.group(1).strip()
    # fallback: 마지막 </file> 이후 텍스트
    parts = text.rsplit("</file>", 1)
    return parts[-1].strip() if len(parts) > 1 else ""


def apply_codex_changes(files: list[dict]) -> list[str]:
    """파일 목록을 디스크에 저장, 변경된 경로 목록 반환"""
    changed = []
    for item in files:
        rel_path = item["path"].lstrip("/\\")
        full_path = PROJECT_ROOT / rel_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(item["content"], encoding="utf-8")
        changed.append(rel_path)
        print(f"    저장됨: {rel_path} ({len(item['content']):,}chars)")
    return changed


# ─────────────────────────────────────────────
# 테스트 / 벤치마크
# ─────────────────────────────────────────────

def run_compile_check(changed_files: list[str]) -> tuple[bool, str]:
    """py_compile 구문 검사"""
    py_files = [f for f in changed_files if f.endswith(".py")]
    if not py_files:
        return True, "검사할 .py 파일 없음"
    ok, out = run_command([PYTHON_BIN, "-m", "py_compile"] + py_files)
    return ok, out if out else "구문 오류 없음"


def run_unit_tests() -> tuple[bool, str]:
    test_file = PROJECT_ROOT / "sr_test_csi_sits_unit.py"
    if not test_file.exists():
        return False, "sr_test_csi_sits_unit.py 아직 없음 — Codex가 생성해야 합니다"
    return run_command([PYTHON_BIN, "sr_test_csi_sits_unit.py"], timeout=600)


def run_benchmark(date: str = "2026-01-12") -> tuple[bool, str]:
    bench_file = PROJECT_ROOT / "sr_benchmark_csi_sits_vs_vrp.py"
    if not bench_file.exists():
        return False, "sr_benchmark_csi_sits_vs_vrp.py 아직 없음 — Codex가 생성해야 합니다"
    out_path = PROJECT_ROOT / "docs" / f"csi_sits_benchmark_{date.replace('-','')}.md"
    ok, out = run_command(
        [PYTHON_BIN, "sr_benchmark_csi_sits_vs_vrp.py",
         "--date", date, "--write", str(out_path)],
        timeout=1800,
    )
    if ok and out_path.exists():
        return True, out_path.read_text(encoding="utf-8")
    return ok, out


# ─────────────────────────────────────────────
# Claude 호출
# ─────────────────────────────────────────────

def _build_claude_prompt(
    round_num: int,
    changed_files: list[str],
    compile_result: tuple[bool, str],
    test_result: tuple[bool, str],
    benchmark_result: tuple[bool, str],
    codex_summary: str,
) -> str:
    compile_ok, compile_out = compile_result
    test_ok,    test_out    = test_result
    bench_ok,   bench_out   = benchmark_result

    # 변경된 파일 내용 (최대 3000자씩)
    file_sections = []
    for rel in changed_files:
        p = PROJECT_ROOT / rel
        if p.exists():
            content = p.read_text(encoding="utf-8")
            snippet = content[:3000] + ("\n...(이하 생략)" if len(content) > 3000 else "")
            file_sections.append(f"### {rel}\n```python\n{snippet}\n```")
    files_ctx = "\n\n".join(file_sections) if file_sections else "(변경 파일 없음)"

    return textwrap.dedent(f"""\
    ## Coordinator Round {round_num} — Claude 검토 요청

    ### Codex 요약
    {codex_summary or '(없음)'}

    ### 변경 파일 목록
    {chr(10).join(f'- {f}' for f in changed_files) or '(없음)'}

    ### 컴파일 검사
    상태: {'PASS' if compile_ok else 'FAIL'}
    ```
    {compile_out[:1000]}
    ```

    ### 단위 테스트
    상태: {'PASS' if test_ok else 'FAIL'}
    ```
    {test_out[:2000]}
    ```

    ### 벤치마크 결과
    상태: {'PASS' if bench_ok else 'FAIL / 미실행'}
    ```
    {bench_out[:3000]}
    ```

    ### 변경된 코드
    {files_ctx}

    ---
    위 내용을 검토하고, docs/UPDATED_BY_CLAUDE.md 형식으로 피드백을 작성해주세요:
    - 항목별 [PASS] / [FAIL] / [WARN] 평가
    - 구체적인 코드 문제 라인 지적
    - 다음 작업 우선순위 P1/P2/P3
    - 성능 목표 달성 여부 (Travel gap <8%, Work Std gap <30%, Max Work gap <12%)
    """)


def call_claude(
    round_num: int,
    changed_files: list[str],
    compile_result: tuple[bool, str],
    test_result: tuple[bool, str],
    benchmark_result: tuple[bool, str],
    codex_summary: str,
    dry_run: bool = False,
) -> str:
    if dry_run:
        print("  [DRY-RUN] Claude 호출 생략")
        return "[DRY-RUN] 피드백 생략"

    try:
        import anthropic
    except ImportError:
        return "[ERROR] anthropic 패키지가 설치되지 않았습니다"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return "[ERROR] ANTHROPIC_API_KEY 환경 변수가 설정되지 않았습니다"

    client  = anthropic.Anthropic(api_key=api_key)
    prompt  = _build_claude_prompt(
        round_num, changed_files,
        compile_result, test_result, benchmark_result,
        codex_summary,
    )

    print(f"  모델: {CLAUDE_MODEL}")
    try:
        message = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as e:
        return f"[ERROR] Claude API call failed: {type(e).__name__}: {e}"
    return message.content[0].text


# ─────────────────────────────────────────────
# 메인 루프
# ─────────────────────────────────────────────

def run_round(
    round_num: int,
    mode: str,
    benchmark_date: str,
    dry_run: bool,
) -> bool:
    """
    한 라운드 실행.
    Returns: True = 성공 조건 달성, False = 계속 필요
    """
    bar = "=" * 55
    print(f"\n{bar}")
    print(f"  라운드 {round_num}  |  모드: {mode}  |  {now_str()}")
    print(f"{bar}")

    changed_files: list[str] = []
    codex_summary: str       = ""

    # ── 1. Codex 구현 ──────────────────────────
    if mode in ("full", "codex-only"):
        print("\n[1/4] Codex 호출 중...")
        files, codex_summary = call_codex(round_num, dry_run=dry_run)
        if files:
            print(f"  {len(files)}개 파일 반환됨")
            changed_files = apply_codex_changes(files)
        else:
            print("  변경 파일 없음")

        # Codex 로그 기록
        codex_log_content = (
            f"Round {round_num} 구현\n\n"
            f"변경 파일: {changed_files or '없음'}\n\n"
            f"{codex_summary}"
        )
        append_log(CODEX_LOG, f"Coordinator Round {round_num}", codex_log_content)
    else:
        print("\n[1/4] Codex 생략 (모드: review/claude-only)")

    # ── 2. 컴파일 검사 ─────────────────────────
    print("\n[2/4] 컴파일 검사 중...")
    if changed_files:
        compile_ok, compile_out = run_compile_check(changed_files)
    else:
        # 이미 존재하는 파일들 검사
        existing_impls = [
            "smart_routing/production_assign_atlanta_csi.py",
            "smart_routing/production_assign_atlanta_sits.py",
        ]
        compile_ok, compile_out = run_compile_check(
            [f for f in existing_impls if (PROJECT_ROOT / f).exists()]
        )
    status = "PASS" if compile_ok else "FAIL"
    print(f"  컴파일: {status}")
    if not compile_ok:
        print(f"  {compile_out[:300]}")

    # ── 3. 단위 테스트 ─────────────────────────
    print("\n[3/4] 단위 테스트 실행 중...")
    test_ok, test_out = run_unit_tests()
    status = "PASS" if test_ok else "FAIL"
    print(f"  테스트: {status}")
    print(f"  {test_out[:300]}")

    # ── 4. 벤치마크 ────────────────────────────
    print("\n[4/4] 벤치마크 실행 중...")
    bench_ok, bench_out = run_benchmark(benchmark_date)
    status = "실행됨" if bench_ok else "FAIL/생략"
    print(f"  벤치마크: {status}")
    if bench_ok:
        # 첫 10줄만 미리보기
        lines = bench_out.splitlines()
        print("  " + "\n  ".join(lines[:10]))

    # ── 5. Claude 검토 ─────────────────────────
    if mode in ("full", "claude-only", "review"):
        print("\n[5/5] Claude 검토 중...")
        feedback = call_claude(
            round_num, changed_files,
            (compile_ok, compile_out),
            (test_ok, test_out),
            (bench_ok, bench_out),
            codex_summary,
            dry_run=dry_run,
        )
        append_log(CLAUDE_LOG, f"Coordinator Round {round_num} 검토", feedback)

        # 성공 판정
        if SUCCESS_CONDITION(feedback):
            print("\n  ✓ 성공 조건 달성")
            return True
        else:
            print("\n  ✗ 아직 개선 필요")
            return False
    else:
        print("\n[5/5] Claude 검토 생략 (모드: codex-only)")
        return compile_ok and test_ok


def main():
    parser = argparse.ArgumentParser(
        description="Claude ↔ Codex 자동 피드백 루프 조율기",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--rounds",          type=int, default=3,
                        help="최대 반복 횟수 (기본값: 3)")
    parser.add_argument("--mode",            default="full",
                        choices=["full", "codex-only", "claude-only", "review"],
                        help="실행 모드 (기본값: full)")
    parser.add_argument("--benchmark-date",  default="2026-01-12",
                        help="벤치마크 기준 날짜 (기본값: 2026-01-12)")
    parser.add_argument("--dry-run",         action="store_true",
                        help="API 호출 없이 흐름만 확인")
    args = parser.parse_args()

    print(f"""
==========================================
  Claude <-> Codex Coordinator
  모드: {args.mode:<10} 라운드: {args.rounds}
  벤치마크: {args.benchmark_date}
  Worker Python: {PYTHON_BIN}
  DRY-RUN: {args.dry_run}
==========================================
""")

    # API 키 확인
    if not args.dry_run:
        missing = []
        if args.mode in ("full", "claude-only", "review") and not os.environ.get("ANTHROPIC_API_KEY"):
            missing.append("ANTHROPIC_API_KEY")
        if args.mode in ("full", "codex-only") and not os.environ.get("OPENAI_API_KEY"):
            missing.append("OPENAI_API_KEY")
        if missing:
            print(f"[ERROR] 다음 환경 변수를 설정하세요: {', '.join(missing)}")
            print("  set ANTHROPIC_API_KEY=sk-ant-...")
            print("  set OPENAI_API_KEY=sk-...")
            sys.exit(1)

    success = False
    for round_num in range(1, args.rounds + 1):
        success = run_round(
            round_num=round_num,
            mode=args.mode,
            benchmark_date=args.benchmark_date,
            dry_run=args.dry_run,
        )
        if success:
            break

    print(f"\n{'='*55}")
    if success:
        print("  최종 결과: 목표 달성")
    else:
        print(f"  최종 결과: {args.rounds}라운드 후 개선 계속 필요")
        print("  → docs/UPDATED_BY_CLAUDE.md에서 최신 피드백 확인")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
