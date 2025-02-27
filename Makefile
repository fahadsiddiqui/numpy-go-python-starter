run:
	./setup.sh
	go build -o numpy-go-python-starter
	./numpy-go-python-starter
clean:
	rm -rf data/
verify:
	if [ ! -d ".venv" ]; then python -m venv .venv; fi
	. .venv/bin/activate && pip install -r requirements.txt && python main.py