CREATE SCHEMA IF NOT EXISTS university;

CREATE TABLE IF NOT EXISTS university.students (
	id INTEGER PRIMARY KEY,
	name VARCHAR(64),
	birthday DATE
);

CREATE TABLE IF NOT EXISTS university.subjects (
	id VARCHAR(4) PRIMARY KEY,
	name VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS university.marks (
	student_id INT,
	subject_id VARCHAR(4),
	mark INT,
	FOREIGN KEY (student_id) REFERENCES university.students (id),
	FOREIGN KEY (subject_id) REFERENCES university.subjects (id)
);
