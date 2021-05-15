notebook:
	docker run -p 8888:8888 --env-file=.env -v $(PWD):/home/jovyan/work jupyter/lat_notebook start-notebook.sh --NotebookApp.token=''

build:
	docker build --rm -t jupyter/lat_notebook .

containers:
	docker ps -a

clean:
	docker system prune