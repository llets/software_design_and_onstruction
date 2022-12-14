create table source_files(
	id integer PRIMARY KEY autoincrement,
	filename varchar(255) NOT NULL,
	processed datetime
);

create table processed_parks (
    id integer PRIMARY KEY autoincrement,
    nameP varchar(255) NOT NULL,
    source_file INT NOT NULL,
    FOREIGN KEY (source_file) REFERENCES source_files(id)
    ON DELETE CASCADE
);

create table processed_neighbourhoods (
    id integer PRIMARY KEY autoincrement,
    nameN varchar(255) NOT NULL,
    source_file INT NOT NULL,
    FOREIGN KEY (source_file) REFERENCES source_files(id)
    ON DELETE CASCADE
);

create table processed_crime_rates (
    id integer PRIMARY KEY autoincrement,
    nameN varchar(255) NOT NULL,
    average_rate FLOAT NOT NULL,
    source_file INT NOT NULL,
    FOREIGN KEY (source_file) REFERENCES source_files(id)
    ON DELETE CASCADE
);

create table points_neighbourhoods(
    id integer PRIMARY KEY autoincrement,
    parent_id integer NOT NULL,
    lattit FLOAT NOT NULL,
    longit FLOAT NOT NULL,
    FOREIGN KEY (parent_id) REFERENCES processed_neighbourhoods(id)
);

create table points_parks(
    id integer PRIMARY KEY autoincrement,
    parent_id integer NOT NULL,
    lattit FLOAT NOT NULL,
    longit FLOAT NOT NULL,
    FOREIGN KEY (parent_id) REFERENCES processed_parks(id)
);