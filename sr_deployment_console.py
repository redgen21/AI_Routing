"""Run with: streamlit run sr_deployment_console.py"""

from deployment_console_ui import render_app


def main(backend: object | None = None) -> None:
    """Render the console; ``backend`` may be injected by smoke tests."""

    render_app(backend)


if __name__ == "__main__":
    main()
