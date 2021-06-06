USE tychy;
CREATE TABLE projects (
    id INT,
    name CHAR(255),
    description CHAR(255)
);

INSERT INTO projects (id, name, description) VALUES (1, 'gumption', 'algorithm and data structure');
INSERT INTO projects (id, name, description) VALUES (2, 'hooligan', 'tiny C compiler by C');
INSERT INTO projects (id, name, description) VALUES (3, 'irenic', 'system programming by GO');
