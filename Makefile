all:

install-server:
	scp coact.cgi dphone3:/home/territory/public_html/
	scp server/table.py dphone3:/home/territory/pycoact/server/
	scp server/geojson.py dphone3:/home/territory/pycoact/server/
