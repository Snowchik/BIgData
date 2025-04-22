CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255),
    email VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE topics (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE messages (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    topic_id INTEGER REFERENCES topics(id),
    content TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_anonymous BOOLEAN DEFAULT FALSE
);

CREATE TABLE logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    action_type VARCHAR(50),
    entity_id INTEGER,
    entity_type VARCHAR(50),
    status VARCHAR(10),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
