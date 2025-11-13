# Slack delete personal DM with the most latest messages first
import time
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# --- Configuration ---
SLACK_TOKEN = "xoxp-your-token-here"
DM_CHANNEL_ID = "D89ABCDEFG-your-dm-channel-id-here"
DELETE_DELAY = 1.2  # seconds between deletions (avoid rate limits)
BATCH_SIZE = 200    # Slack returns up to 200 msgs per page

client = WebClient(token=SLACK_TOKEN)

def delete_messages(channel_id):
    cursor = None
    total_deleted = 0

    while True:
        try:
            # Fetch one "page" of messages
            response = client.conversations_history(
                channel=channel_id,
                limit=BATCH_SIZE,
                cursor=cursor
            )

            messages = response.get("messages", [])
            if not messages:
                print("No more messages found.")
                break

            for msg in messages:
                # Delete only messages you sent
                if msg.get("user") and msg["user"] != "USLACKBOT":
                    ts = msg["ts"]
                    try:
                        client.chat_delete(channel=channel_id, ts=ts)
                        total_deleted += 1
                        print(f"Deleted message {total_deleted}: ts={ts}")
                        time.sleep(DELETE_DELAY)
                    except SlackApiError as e:
                        if e.response["error"] == "ratelimited":
                            retry_after = int(e.response.headers.get("Retry-After", 30))
                            print(f"Rate limited. Sleeping for {retry_after}s...")
                            time.sleep(retry_after)
                        else:
                            print(f"Error deleting message {ts}: {e.response['error']}")
                            time.sleep(DELETE_DELAY)

            # Check for more pages
            cursor = response.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                print("All messages processed.")
                break

        except SlackApiError as e:
            print(f"Error fetching messages: {e.response['error']}")
            break

    print(f"✅ Finished. Total messages deleted: {total_deleted}")

if __name__ == "__main__":
    delete_messages(DM_CHANNEL_ID)
