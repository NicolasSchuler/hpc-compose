Place your `.gguf` model file here when trying the `llama-app.yaml` or `llm-curl-workflow.yaml` example.

The example command expects:

- `./models/model.gguf`

For the home-directory LLM example in [`../llm-curl-workflow-workdir.yaml`](../llm-curl-workflow-workdir.yaml), place the file at:

- `$HOME/models/model.gguf`

If the file is a symlink, make sure the target is also visible inside the mounted directory. Copying the real `.gguf` file is the safest option.
