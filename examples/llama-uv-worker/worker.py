# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "openai>=1.0.0",
# ]
# ///

import os
from pathlib import Path

from openai import OpenAI


def main() -> None:
    client = OpenAI(
        base_url=os.environ["OPENAI_BASE_URL"],
        api_key="unused",
    )
    response = client.chat.completions.create(
        model=os.environ["MODEL_NAME"],
        messages=[
            {"role": "system", "content": "You are a concise assistant."},
            {
                "role": "user",
                "content": "Explain why shared cache paths matter when prepare runs on a login node and the job runs on a compute node.",
            },
        ],
        max_tokens=80,
        temperature=0.2,
    )
    print(response.choices[0].message.content, flush=True)
    Path(os.environ["REQUEST_DONE_PATH"]).write_text("done\n", encoding="utf-8")


if __name__ == "__main__":
    main()
