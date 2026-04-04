import os
import time


def main() -> None:
    base_url = os.environ.get("LLM_BASE_URL", "http://127.0.0.1:8080/v1")
    print(f"app is configured to talk to {base_url}", flush=True)
    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
