# Init project
```bash
# Install quix-cli
curl -fsSL https://github.com/quixio/quix-cli/raw/main/install.sh | bash

# init
quix local init

# up and run tech stacks
# Executing 'docker compose -f compose.local.yaml up --build -d --remove-orphans kafka_broker'
quix local pipeline up
quix local pipeline down
```

# Create apps
```bash
# app: silver-sink
quix local apps create
```

# Check list context
```bash
quix contexts list
quix contexts broker set
quix local init --project-root .
```

# Deployment
```bash
quix local pipeline update
quix local pipeline up

# prepare mlflow services
cd mlflow-services/
docker-compose up -d

# stop services
docker-compose down
cd ..
quix local pipeline down
```

# View pipeline
```bash
quix pipeline view
```

# Also can run pipeline locally
```bash
cd ef-feature-engineering/

# install uv 
uv venv --python 3.11
source .venv/bin/activate

# install dependencies
uv pip install -r requirements.txt

# run the app
python main.py
```