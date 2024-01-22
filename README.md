### ELT dbt

The initial batch of data is generated and is under seeds folder, which is for all intents and purposes ignored from the repo. <br>
After that, each hour/day/week there's an additional CSV file depenging on the parameters set. <br>

The ingestion script will scan through the /seeds directory and ignore the files the tables for which already exist. <br>
If ingestion fails it will send a message to a Telegram bot with respective error. It is kinda cool ngl <br>

![look_at_this](tg_bot_msg.png)
