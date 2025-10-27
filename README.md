# OpenSearch Diagnostic Chatbot

This project is a troubleshooting chatbot for OpenSearch clusters.  It uses **Amazon Lex**, **AWS Lambda**, and **AWS4Auth** to check cluster health and guide users through fixes for yellow and red cluster states.

---

## 📌 Overview

The chatbot connects to OpenSearch clusters and runs checks on:
- Disk space
- JVM and CPU usage
- Replica configuration
- Node health

It then gives clear steps to fix common issues.  
This helps reduce manual work and improves resolution time.

---

## 🧠 Features

- Real-time cluster metric retrieval  
- Automated troubleshooting for yellow and red states  
- Step-by-step guided responses  
- Environment variable support for secure configuration  
- Debug mode for development

---

## 🛠️ Tech Stack

- **AWS Lambda**  
- **Amazon Lex**  
- **OpenSearch Service**  
- **Boto3 / AWS4Auth**  
- **Python 3.10+**

---

## ⚙️ Setup

### 1. Clone the repo
```bash
git clone https://github.com/your-username/opensearch-chatbot.git
cd opensearch-chatbot
```

### 2. Create a virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set environment variables
```bash
export AWS_REGION=us-east-1
export CLUSTERS='{"demo-yellow":"https://search-demo-yellow.example.com"}'
export DEBUG=false
```

### 5. Deploy to Lambda.
Use your preferred method (SAM, CDK, Serverless Framework, or manual upload).

### 6. Amazon Lex Setup

This project uses **Amazon Lex** as the front end for the chatbot.

To set it up:
1. Create a new Lex bot in the AWS console.  
2. Add an intent (for example: `DiagnoseClusterIntent`).  
3. Configure slots for `ClusterName` and `UserResponse` if needed.  
4. Set the bot’s Lambda fulfillment function to this Lambda.  
5. Build and test the bot.

Once linked, Lex will pass the user’s input to the Lambda function and return the troubleshooting steps.


## 🔐 Security Notes
- No real cluster endpoints or secrets should be committed.
- Use environment variables for any private data.
- Do not log sensitive session information.
- Use AWS IAM roles for authentication.


## 📝 Future Improvements
- Add support for multi-region monitoring
- Add more advanced troubleshooting logic by implementing it with other AWS services (Amazon DynamoDB, RDS, etc.)
- Create a simple dashboard UI
