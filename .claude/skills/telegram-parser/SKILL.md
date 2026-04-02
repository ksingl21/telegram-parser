---
name: telegram-parser
description: Run the Telegram chat parser to fetch new messages from the crossfit group, classify them by theme using Ollama, and update the Excel file.
allowed-tools: Bash
disable-model-invocation: true
---

Run the Telegram parser script to fetch new messages from @nkabir6202 in the crossfit group, classify them by theme using the local Ollama LLM, and update the Excel file at ~/Documents/crossfit_messages.xlsx.

Steps:
1. Make sure Ollama is running
2. Run the parser script
3. Report how many messages were fetched and where the Excel file is saved

```bash
cd /Users/kapilsingla/Documents/telegram-parser && ollama serve &>/dev/null & sleep 2 && python3 telegram_parser.py
```
