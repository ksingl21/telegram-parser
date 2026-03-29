import asyncio
import re
import json
import os
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import MessageMediaDocument, MessageMediaPhoto
import ollama
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ──────────────────────────────────────────────────────────────────
API_ID    = os.getenv("API_ID")    # set in .env
API_HASH  = os.getenv("API_HASH")  # set in .env
GROUP     = "crossfit"
FRIEND    = "nkabir6202"
OUTPUT    = "/Users/kapilsingla/Documents/crossfit_messages.xlsx"
MODEL     = "llama3.2"

KNOWN_THEMES = [
    "Educational",
    "AI",
    "Tech Advancement",
    "Politics",
    "Audiobooks / PDF Books",
    "Other",
]
# ────────────────────────────────────────────────────────────────────────────

URL_REGEX = re.compile(r'https?://[^\s]+')

THEME_COLORS = {
    "Educational":          "D9EAD3",
    "AI":                   "CFE2F3",
    "Tech Advancement":     "D0E0E3",
    "Politics":             "FCE5CD",
    "Audiobooks / PDF Books": "EAD1DC",
    "Other":                "F3F3F3",
}

def get_fill(theme: str) -> PatternFill:
    color = THEME_COLORS.get(theme, "FFFFFF")
    return PatternFill(start_color=color, end_color=color, fill_type="solid")


def classify_message(text: str, known_themes: list[str]) -> str:
    """Ask Ollama to classify a message into one of the known themes."""
    themes_str = "\n".join(f"- {t}" for t in known_themes)
    prompt = f"""You are a message classifier. Given the message below, pick the SINGLE most fitting theme from the list.
If none fit well, return "Other".
Reply with ONLY the theme name, nothing else.

Themes:
{themes_str}

Message:
{text[:1000]}

Theme:"""
    try:
        response = ollama.chat(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response["message"]["content"].strip().strip('"').strip("'")
        # fuzzy-match to known themes
        for t in known_themes:
            if t.lower() in raw.lower() or raw.lower() in t.lower():
                return t
        return raw if raw else "Other"
    except Exception as e:
        print(f"  [warn] Ollama error: {e}")
        return "Other"


def discover_extra_themes(messages_sample: list[str]) -> list[str]:
    """Ask Ollama to suggest additional themes from a sample of messages."""
    combined = "\n---\n".join(m[:300] for m in messages_sample[:40])
    prompt = f"""Below are messages from a Telegram group. Identify any recurring topics or themes NOT in this list:
{chr(10).join("- " + t for t in KNOWN_THEMES)}

Messages sample:
{combined}

Reply with a JSON array of new theme names only, e.g. ["Health & Fitness", "Finance"].
If there are none, reply with [].
Only theme names, no explanation."""
    try:
        response = ollama.chat(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}]
        )
        raw = response["message"]["content"].strip()
        match = re.search(r'\[.*?\]', raw, re.DOTALL)
        if match:
            return json.loads(match.group())
        return []
    except Exception as e:
        print(f"  [warn] Theme discovery error: {e}")
        return []


async def fetch_messages(client, group: str, friend: str) -> list[dict]:
    """Fetch all messages from `friend` in `group`."""
    print(f"Looking up group: {group}")
    entity = await client.get_entity(group)

    print(f"Fetching messages from @{friend} ...")
    rows = []
    async for msg in client.iter_messages(entity, from_user=friend, limit=None):
        if not msg.text and not msg.media:
            continue

        text = msg.text or ""
        urls = URL_REGEX.findall(text)

        # web preview / inline url
        if msg.entities:
            from telethon.tl.types import MessageEntityUrl, MessageEntityTextUrl
            for ent in msg.entities:
                if isinstance(ent, MessageEntityTextUrl):
                    urls.append(ent.url)
                elif isinstance(ent, MessageEntityUrl):
                    urls.append(text[ent.offset: ent.offset + ent.length])

        # deduplicate urls
        urls = list(dict.fromkeys(urls))

        doc_name = ""
        doc_type = ""
        if isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            for attr in doc.attributes:
                from telethon.tl.types import DocumentAttributeFilename, DocumentAttributeAudio
                if hasattr(attr, "file_name") and attr.file_name:
                    doc_name = attr.file_name
                if isinstance(attr, DocumentAttributeAudio):
                    doc_type = "Audio"
            if not doc_type:
                mime = getattr(doc, "mime_type", "")
                if "pdf" in mime:
                    doc_type = "PDF"
                elif "audio" in mime:
                    doc_type = "Audio"
                elif "video" in mime:
                    doc_type = "Video"
                else:
                    doc_type = "Document"

        rows.append({
            "date":     msg.date.strftime("%Y-%m-%d %H:%M"),
            "text":     text,
            "urls":     "\n".join(urls),
            "doc_name": doc_name,
            "doc_type": doc_type,
            "theme":    "",
        })

    print(f"Fetched {len(rows)} messages.")
    return rows


def build_excel(rows: list[dict], output_path: str):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Messages by Theme"

    headers = ["Date", "Message", "Theme", "URLs", "Document Name", "Doc Type"]
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="2F4F4F", end_color="2F4F4F", fill_type="solid")

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")

    # sort by theme then date
    rows_sorted = sorted(rows, key=lambda r: (r["theme"], r["date"]))

    for row_idx, r in enumerate(rows_sorted, start=2):
        fill = get_fill(r["theme"])
        values = [r["date"], r["text"], r["theme"], r["urls"], r["doc_name"], r["doc_type"]]
        for col_idx, val in enumerate(values, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.fill = fill
            cell.alignment = Alignment(wrap_text=True, vertical="top")

    # column widths
    widths = [18, 80, 22, 50, 35, 12]
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions

    # ── Summary sheet ────────────────────────────────────────────────────────
    ws2 = wb.create_sheet("Theme Summary")
    from collections import Counter
    counts = Counter(r["theme"] for r in rows)
    ws2.append(["Theme", "Message Count"])
    ws2["A1"].font = Font(bold=True)
    ws2["B1"].font = Font(bold=True)
    for theme, count in sorted(counts.items()):
        ws2.append([theme, count])
    ws2.column_dimensions["A"].width = 30
    ws2.column_dimensions["B"].width = 15

    wb.save(output_path)
    print(f"\nSaved → {output_path}")


async def main():
    async with TelegramClient("session_crossfit", API_ID, API_HASH) as client:
        rows = await fetch_messages(client, GROUP, FRIEND)

        if not rows:
            print("No messages found. Check group name and username.")
            return

        # discover extra themes from a sample
        print("\nDiscovering additional themes with Ollama...")
        texts = [r["text"] for r in rows if r["text"]]
        extra = discover_extra_themes(texts)
        all_themes = KNOWN_THEMES[:-1] + extra + ["Other"]  # keep Other last
        # deduplicate
        seen = set()
        unique_themes = []
        for t in all_themes:
            if t.lower() not in seen:
                seen.add(t.lower())
                unique_themes.append(t)
        print(f"Themes to use: {unique_themes}")

        # add new theme colors for discovered ones
        palette = ["FFF2CC", "E6D0DE", "D9D2E9", "C9DAF8", "B6D7A8", "FFE599"]
        for i, t in enumerate(extra):
            if t not in THEME_COLORS:
                THEME_COLORS[t] = palette[i % len(palette)]

        # classify each message
        print("\nClassifying messages (this may take a few minutes)...")
        for i, row in enumerate(rows):
            content = row["text"] or row["doc_name"] or row["doc_type"]
            if not content.strip():
                row["theme"] = "Other"
                continue
            row["theme"] = classify_message(content, unique_themes)
            print(f"  [{i+1}/{len(rows)}] {row['theme'][:30]:<30} | {content[:60]}")

        build_excel(rows, OUTPUT)
        print("\nDone! Open crossfit_messages.xlsx")


if __name__ == "__main__":
    asyncio.run(main())
