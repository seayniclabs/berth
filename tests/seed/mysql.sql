-- Berth test seed data — MySQL
-- Mirror of postgres.sql for cross-database testing

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    role VARCHAR(50) DEFAULT 'user',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT,
    product VARCHAR(200) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_status ON orders(status);

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

INSERT INTO orders (user_id, product, amount, status) VALUES
    (1, 'Product A', 125.00, 'pending'),
    (1, 'Product B', 89.99, 'shipped'),
    (1, 'Product C', 210.50, 'delivered'),
    (1, 'Product D', 45.00, 'cancelled'),
    (1, 'Product E', 330.00, 'pending'),
    (2, 'Product A', 99.99, 'shipped'),
    (2, 'Product B', 150.00, 'delivered'),
    (2, 'Product C', 75.50, 'pending'),
    (2, 'Product D', 200.00, 'cancelled'),
    (2, 'Product E', 180.00, 'shipped'),
    (3, 'Product A', 55.00, 'pending'),
    (3, 'Product B', 420.00, 'delivered'),
    (3, 'Product C', 88.00, 'shipped'),
    (3, 'Product D', 15.99, 'pending'),
    (3, 'Product E', 300.00, 'cancelled'),
    (4, 'Product A', 175.00, 'shipped'),
    (4, 'Product B', 60.00, 'delivered'),
    (4, 'Product C', 140.00, 'pending'),
    (4, 'Product D', 95.00, 'shipped'),
    (4, 'Product E', 250.00, 'delivered'),
    (5, 'Product A', 110.00, 'pending'),
    (5, 'Product B', 85.00, 'cancelled'),
    (5, 'Product C', 195.00, 'shipped'),
    (5, 'Product D', 70.00, 'delivered'),
    (5, 'Product E', 310.00, 'pending'),
    (6, 'Product A', 45.00, 'shipped'),
    (6, 'Product B', 225.00, 'pending'),
    (6, 'Product C', 160.00, 'delivered'),
    (6, 'Product D', 90.00, 'cancelled'),
    (6, 'Product E', 135.00, 'shipped'),
    (7, 'Product A', 280.00, 'delivered'),
    (7, 'Product B', 50.00, 'pending'),
    (7, 'Product C', 175.00, 'shipped'),
    (7, 'Product D', 120.00, 'pending'),
    (7, 'Product E', 65.00, 'cancelled'),
    (8, 'Product A', 200.00, 'shipped'),
    (8, 'Product B', 145.00, 'delivered'),
    (8, 'Product C', 80.00, 'pending'),
    (8, 'Product D', 350.00, 'shipped'),
    (8, 'Product E', 40.00, 'cancelled'),
    (9, 'Product A', 165.00, 'pending'),
    (9, 'Product B', 95.00, 'delivered'),
    (9, 'Product C', 230.00, 'shipped'),
    (9, 'Product D', 55.00, 'pending'),
    (9, 'Product E', 185.00, 'delivered'),
    (10, 'Product A', 75.00, 'cancelled'),
    (10, 'Product B', 290.00, 'shipped'),
    (10, 'Product C', 110.00, 'pending'),
    (10, 'Product D', 205.00, 'delivered'),
    (10, 'Product E', 60.00, 'shipped');
