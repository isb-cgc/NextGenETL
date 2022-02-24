install:
	sudo apt-get install wget
	sudo apt-get install mariadb-server
	sudo mysql_secure_installation
	sudo mysql -u root < ./reset-root.sql

init:
	mysql -u root -D mysql < ./init-targetome.sql
	wget https://github.com/ablucher/The-Cancer-Targetome/raw/master/database/Druggability_070717.sql
	mysql -u user -D targetome < ./Druggability_070717.sql

export:
	./targetome-etl-flatten.sh
