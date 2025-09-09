import pandas as pd
from telethon import TelegramClient
import asyncio
import csv
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest
import os

# ====== Config ======
api_id = 26195766  
api_hash = '3baaf17fce7e4842346158a3fd93f8a1' 
session_name = "telegram_session"  
message_limit = 200000  # Max messages à récupérer
# =====================

# Charger les URLs uniques et noms depuis le CSV
file_path = '10s25___indignons_nous__.csv'
df = pd.read_csv(file_path)
df_unique = df.drop_duplicates(subset=['url'])

# Créer le client Telegram
client = TelegramClient(session_name, api_id, api_hash)

# ====== FONCTIONS ======
async def scrap_message(client, channel, file_name, limit=100000):
    messages = []
    try:
        entity = await client.get_entity(channel)
        async for message in client.iter_messages(channel, limit=limit):
            if message.text:
                messages.append({
                    "chat": channel,
                    "chat_name": getattr(entity, "title", "Inconnu"),
                    "message_id": message.id,
                    "date": message.date.isoformat() if message.date else None,
                    "sender_id": message.sender_id,
                    "text": message.message
                })
    except Exception as e:
        print(f"Erreur lors du scraping de {channel}: {e}")
        return

    # Sauvegarder dans un CSV spécifique pour le canal
    output_dir = "scraped_messages_v1"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, file_name)

    with open(output_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["chat", "chat_name", "message_id", "date", "sender_id", "text"])
        writer.writeheader()
        writer.writerows(messages)

    print(f"{len(messages)} messages savec in {output_path}")

async def join_channel(client, channel_link):
    try:
        await client(JoinChannelRequest(channel_link))
        print(f"Joined : {channel_link}")
    except:
        try:
            # Essayer via ImportChatInviteRequest pour les liens d'invitation privés
            invite_hash = channel_link.split('+')[1]
            await client(ImportChatInviteRequest(invite_hash))
            print(f"Invitation acceptée : {channel_link}")
        except Exception as e:
            print(f"Impossible de rejoindre {channel_link} : {e}")

async def main():
    i = 0
    for index, row in df_unique.iterrows():

        #if i == 20:
         #   print("Pause de 120 secondes pour éviter les limites...")
         #   await asyncio.sleep(120)
         #   i = 0  # Réinitialisation du compteur


        url = row['url']
        nom = row['nom'] if pd.notna(row['nom']) else 'Nom_inconnu'
        nom_clean = "".join(c for c in nom if c.isalnum() or c in (' ', '_')).strip().replace(" ", "_")
        file_name = f"messages_{nom_clean}.csv"

        print(f"\nTraitement du canal : {nom} ({url})")

        await join_channel(client, url)
        await scrap_message(client, url, file_name, limit=message_limit)
        await asyncio.sleep(450)
        i = i+1

with client:

    client.loop.run_until_complete(main())

