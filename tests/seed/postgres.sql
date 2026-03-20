-- Berth test seed data — PostgreSQL
-- Creates a minimal schema for integration testing

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    role VARCHAR(50) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    product VARCHAR(200) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);

-- 10 test users
INSERT INTO users (name, email, role) VALUES
    ('Alice Test', 'alice@test.local', 'admin'),
    ('Bob Test', 'bob@test.local', 'user'),
    ('Carol Test', 'carol@test.local', 'user'),
    ('Dave Test', 'dave@test.local', 'editor'),
    ('Eve Test', 'eve@test.local', 'user'),
    ('Frank Test', 'frank@test.local', 'user'),
    ('Grace Test', 'grace@test.local', 'admin'),
    ('Hank Test', 'hank@test.local', 'user'),
    ('Iris Test', 'iris@test.local', 'editor'),
    ('Jack Test', 'jack@test.local', 'user');

-- 50 test orders (5 per user)
INSERT INTO orders (user_id, product, amount, status)
SELECT
    u.id,
    'Product ' || gs.n,
    (random() * 500 + 10)::decimal(10,2),
    CASE (gs.n % 4)
        WHEN 0 THEN 'pending'
        WHEN 1 THEN 'shipped'
        WHEN 2 THEN 'delivered'
        WHEN 3 THEN 'cancelled'
    END
FROM users u
CROSS JOIN generate_series(1, 5) AS gs(n);
