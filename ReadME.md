### Slack DM Deleter (Earliest Messages First)

A Python script to delete your own messages in a Slack DM safely.
Deletes messages **earliest first** while handling rate limits, batching, optional date filtering, and dry-run mode.

Step 1: Create a Slack App
1. Go to https://api.slack.com/apps
2. Click Create New App → From Scratch
3. Name it e.g. DeletePersonalDMs and pick your workspace

Step 2: Configure OAuth Scopes
1. In the app sidebar, go to Features → OAuth & Permissions
2. Under User Token Scopes, click Add an OAuth Scope
3. Add the following scopes:
   `chat:write`
   `im:history`
   (Optional: add `channels:history` if deleting in public channels)
4. Go to Settings → Install App and click Install to Workspace_Name / Reinstall to Workspace_Name, approve, and copy your User OAuth Token (xoxp-...)

Step 3: Find Your DM Channel ID\
Option 1: From the URL
1. Open the DM in Slack
2. Check the URL, e.g.:
   https://app.slack.com/client/T01234567/D89ABCDEFG
3. Your DM channel ID is the part starting with D, e.g., D89ABCDEFG

Option 2: From Slack App UI
1. Open the DM in the Slack desktop
2. Click the More actions (three dots) in the top right corner of the conversation
3. Click Open conversation details
4. In the details panel, look for **Channel ID:** - this is your DM channel ID

Step 4: Save the Script\
Create `slack_delete_messages.py` and paste the following code:
```
import time
import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# --- CONFIGURATION ---
SLACK_TOKEN = "xoxp-your-token-here"
DM_CHANNEL_ID = "D89ABCDEFG-your-dm-channel-id-here"
DELETE_DELAY = 1.2        # seconds between deletions (rate-limit safe)
BATCH_SIZE = 200          # messages per API call
DELETE_OLDER_THAN_DAYS = 30  # 0 = delete all messages
DRY_RUN = False           # True = list messages without deleting
# ----------------------

client = WebClient(token=SLACK_TOKEN)

def fetch_all_messages(channel_id):
    """Fetch all messages in the channel and return sorted earliest first."""
    all_messages = []
    cursor = None

    while True:
        try:
            response = client.conversations_history(
                channel=channel_id,
                limit=BATCH_SIZE,
                cursor=cursor
            )
            msgs = response.get("messages", [])
            if not msgs:
                break
            all_messages.extend(msgs)
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                break
        except SlackApiError as e:
            print(f"Error fetching messages: {e.response['error']}")
            break

    # sort messages by timestamp ascending (earliest first)
    all_messages.sort(key=lambda m: float(m["ts"]))
    return all_messages

def delete_messages_earliest_first(channel_id):
    total_deleted = 0
    total_skipped = 0
    my_id = client.auth_test()["user_id"]

    # Optional cutoff for date filtering
    cutoff_ts = None
    if DELETE_OLDER_THAN_DAYS > 0:
        cutoff_ts = time.time() - (DELETE_OLDER_THAN_DAYS * 24 * 60 * 60)
        cutoff_date = datetime.datetime.fromtimestamp(cutoff_ts).strftime("%Y-%m-%d")
        print(f"Deleting messages older than {DELETE_OLDER_THAN_DAYS} days (before {cutoff_date})")
    else:
        print("Deleting all messages (no date filter)")

    messages = fetch_all_messages(channel_id)
    if not messages:
        print("No messages found.")
        return

    for msg in messages:
        ts = msg["ts"]
        user = msg.get("user")

        # Skip messages not sent by you
        if user != my_id:
            total_skipped += 1
            continue

        # Skip messages newer than cutoff
        if cutoff_ts and float(ts) > cutoff_ts:
            total_skipped += 1
            continue

        if DRY_RUN:
            print(f"[DRY RUN] Would delete message ts={ts}")
            total_deleted += 1
            continue

        try:
            client.chat_delete(channel=channel_id, ts=ts)
            total_deleted += 1
            print(f"Deleted message {total_deleted} (ts={ts})")
            time.sleep(DELETE_DELAY)

        except SlackApiError as e:
            error = e.response["error"]
            if error == "ratelimited":
                retry_after = int(e.response.headers.get("Retry-After", 30))
                print(f"Rate limited. Sleeping {retry_after}s...")
                time.sleep(retry_after)
            elif error == "cant_delete_message":
                total_skipped += 1
                continue
            else:
                print(f"Error deleting message {ts}: {error}")
                time.sleep(DELETE_DELAY)

    print(f"Finished! Deleted: {total_deleted}, Skipped: {total_skipped}")

if __name__ == "__main__":
    delete_messages_earliest_first(DM_CHANNEL_ID)

```

Step 5: Install Dependencies\
Open a terminal in the folder containing the script and run:
```
pip install slack_sdk
```
or
```
pip3 install slack_sdk
```

Step 6: Run the Script
```
python3 slack_delete_messages.py
```
Expected output:
```
Deleting messages older than 30 days (before 2025-11-11)
Deleted message 1 (ts=1762841505.592539)
Deleted message 2 (ts=1762842521.855909)
Finished! Deleted: 25, Skipped: 8
```
Notes:
- cant_delete_message errors = message not yours (normal)
- Script auto-pauses on rate limits
- Stop anytime with `Ctrl + C`
- Deleted messages are permanent
- Adjust `DELETE_OLDER_THAN_DAYS` to:\
   30 → delete messages older than 30 days\
   0 → delete all messages
- Set `DRY_RUN = True` to see what would be deleted without actually deleting

> [!CAUTION]
> Use responsibly. Deleted messages are permanent. The author is not responsible for data loss.

Made with ❤️ in Kuala Lumpur.

### License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
