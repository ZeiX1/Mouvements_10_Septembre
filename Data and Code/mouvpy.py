import pandas as pd
from telethon import TelegramClient
import asyncio
import json
import os
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
import time
# ====== Config ======
api_id = 1234 #mettre son propre id  
api_hash = 'abcd' #mettre son propre api hash
session_name = "telegram_session"  
csv_path = "10s25___indignons_nous__.csv"  
output_csv = "telegram_messages.csv"
message_limit = None  
# ===========================

async def main():
    # Check csv exists
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Le fichier {csv_path} n'existe pas.")

    df = pd.read_csv(csv_path)
    
    telegram_links = df['url'].dropna().unique().tolist()
    print(f"Col : {df['url']} | {len(telegram_links)} liens trouvés")
    
    #Telethon initialisation
    client = TelegramClient(session_name, api_id, api_hash)
    await client.start()
    
    all_data = []
    for link in telegram_links:
        print(f" Downloading messages from {link} ...")
        try:
            # 2 distinct cases, if the chat is private or public
            if "joinchat" in link:
                # Private
                invite_hash = link.split("joinchat/")[-1]
                await client(ImportChatInviteRequest(invite_hash))
            elif "/+" in link:
                 invite_hash = link.split("/+")[-1]
                 await client(ImportChatInviteRequest(invite_hash))
            
            else:
                # Public
                username = link.split("t.me/")[-1]
                await client(JoinChannelRequest(username))

            # Get entity (channel, )
            entity = await client.get_entity(link)
            async for message in client.iter_messages(entity, limit=message_limit):
                all_data.append({
                    "chat": link,
                    "chat_name": getattr(entity, "title", "Inconnu"),
                    "message_id": message.id,
                    "date": message.date.isoformat() if message.date else None,
                    "sender_id": message.sender_id,
                    "text": message.message
                })
        except Exception as e:
            print(f"Error for {link} : {e}")
        
        time.sleep(1)
        
    
    await client.disconnect()
    
    # Save data as a CSV (might be more relevant to use JSON)
    if all_data:
        messages_df = pd.DataFrame(all_data)
        messages_df.to_csv(output_csv, index=False, encoding="utf-8")
        print(f"✅ {len(messages_df)} messages saved in  {output_csv}")
    else:
        print(" Error, no message detected.")

if __name__ == "__main__":

    asyncio.run(main())
