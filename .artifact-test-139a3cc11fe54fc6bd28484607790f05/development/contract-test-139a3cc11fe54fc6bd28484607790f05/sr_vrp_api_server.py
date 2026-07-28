"""Stable root entrypoint for the Smart Routing API service."""

from services.api.sr_vrp_api_server import main


if __name__ == "__main__":
    main()
