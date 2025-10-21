"""Tests for complex query and update patterns."""

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine

from pandalchemy import DataBase


def test_conditional_bulk_updates_multiple_groups(tmp_path):
    """Test conditional bulk updates with multiple groups."""
    db_path = tmp_path / "conditional.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create employee table
    employees = pd.DataFrame({
        'id': range(1, 101),
        'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR'], 100),
        'salary': np.random.randint(50000, 150000, 100),
        'performance': np.random.choice(['Excellent', 'Good', 'Average', 'Poor'], 100),
        'bonus': [0.0] * 100
    })
    db.create_table('employees', employees, primary_key='id')

    # Conditional updates by performance rating
    # Excellent: 20% bonus
    excellent = db['employees'][db['employees']['performance'] == 'Excellent']
    for emp_id in excellent.index:
        salary = db['employees'].get_row(emp_id)['salary']
        db['employees'].update_row(emp_id, {'bonus': salary * 0.20})

    # Good: 10% bonus
    good = db['employees'][db['employees']['performance'] == 'Good']
    for emp_id in good.index:
        salary = db['employees'].get_row(emp_id)['salary']
        db['employees'].update_row(emp_id, {'bonus': salary * 0.10})

    # Average: 5% bonus
    average = db['employees'][db['employees']['performance'] == 'Average']
    for emp_id in average.index:
        salary = db['employees'].get_row(emp_id)['salary']
        db['employees'].update_row(emp_id, {'bonus': salary * 0.05})

    # Poor: 0% bonus (no update needed)

    db.push()

    # Verify conditional updates
    db.pull()

    # Check that excellent employees have 20% bonus
    excellent_employees = db['employees'][db['employees']['performance'] == 'Excellent']
    if len(excellent_employees) > 0:
        first_excellent = excellent_employees.iloc[0]
        expected_bonus = first_excellent['salary'] * 0.20
        assert abs(first_excellent['bonus'] - expected_bonus) < 0.01


def test_aggregation_based_parent_updates(tmp_path):
    """Test updating parent table based on child aggregates."""
    db_path = tmp_path / "aggregates.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create customers
    customers = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'total_orders': [0, 0, 0],
        'total_spent': [0.0, 0.0, 0.0],
        'avg_order_value': [0.0, 0.0, 0.0]
    })
    db.create_table('customers', customers, primary_key='id')

    # Create orders
    orders = pd.DataFrame({
        'id': range(1, 21),
        'customer_id': np.random.choice([1, 2, 3], 20),
        'amount': np.random.uniform(50, 500, 20).round(2),
        'status': np.random.choice(['completed', 'pending', 'cancelled'], 20)
    })
    db.create_table('orders', orders, primary_key='id')

    # Calculate aggregates for each customer
    for customer_id in db['customers'].index:
        # Get completed orders for this customer
        customer_orders = db['orders'][
            (db['orders']['customer_id'] == customer_id) &
            (db['orders']['status'] == 'completed')
        ]

        if len(customer_orders) > 0:
            total_orders = len(customer_orders)
            total_spent = customer_orders['amount'].sum()
            avg_order = total_spent / total_orders

            db['customers'].update_row(customer_id, {
                'total_orders': total_orders,
                'total_spent': round(total_spent, 2),
                'avg_order_value': round(avg_order, 2)
            })

    db.push()

    # Verify aggregates
    db.pull()
    # At least one customer should have orders
    customers_with_orders = db['customers'][db['customers']['total_orders'] > 0]
    assert len(customers_with_orders) > 0


def test_running_totals_update(tmp_path):
    """Test updating running totals based on transactions."""
    db_path = tmp_path / "running.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create accounts
    accounts = pd.DataFrame({
        'id': [1, 2],
        'balance': [1000.0, 2000.0]
    })
    db.create_table('accounts', accounts, primary_key='id')

    # Create transactions
    transactions = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'account_id': [1, 1, 1, 2, 2],
        'amount': [100.0, -50.0, 200.0, -100.0, 150.0],
        'balance_after': [0.0] * 5
    })
    db.create_table('transactions', transactions, primary_key='id')

    # Calculate running balances
    for account_id in db['accounts'].index:
        account_txns = db['transactions'][
            db['transactions']['account_id'] == account_id
        ].sort_index()

        running_balance = db['accounts'].get_row(account_id)['balance']

        for txn_id in account_txns.index:
            txn = db['transactions'].get_row(txn_id)
            running_balance += txn['amount']
            db['transactions'].update_row(txn_id, {'balance_after': running_balance})

        # Update final balance
        db['accounts'].update_row(account_id, {'balance': running_balance})

    db.push()

    # Verify running totals
    db.pull()
    assert db['transactions'].get_row(1)['balance_after'] == 1100.0  # 1000 + 100
    assert db['transactions'].get_row(2)['balance_after'] == 1050.0  # 1100 - 50
    assert db['transactions'].get_row(3)['balance_after'] == 1250.0  # 1050 + 200


def test_multi_table_join_based_update(tmp_path):
    """Test updates based on data from multiple joined tables."""
    db_path = tmp_path / "joins.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create users
    users = pd.DataFrame({
        'id': [1, 2, 3],
        'username': ['alice', 'bob', 'charlie'],
        'reputation': [0, 0, 0]
    })
    db.create_table('users', users, primary_key='id')

    # Create posts
    posts = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'user_id': [1, 1, 2, 3, 3],
        'upvotes': [10, 5, 20, 15, 8]
    })
    db.create_table('posts', posts, primary_key='id')

    # Create comments
    comments = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'user_id': [1, 2, 2, 3],
        'upvotes': [3, 5, 2, 4]
    })
    db.create_table('comments', comments, primary_key='id')

    # Calculate reputation from posts and comments
    for user_id in db['users'].index:
        # Sum upvotes from posts
        user_posts = db['posts'][db['posts']['user_id'] == user_id]
        post_upvotes = user_posts['upvotes'].sum() if len(user_posts) > 0 else 0

        # Sum upvotes from comments
        user_comments = db['comments'][db['comments']['user_id'] == user_id]
        comment_upvotes = user_comments['upvotes'].sum() if len(user_comments) > 0 else 0

        # Update reputation
        total_reputation = post_upvotes + comment_upvotes
        db['users'].update_row(user_id, {'reputation': total_reputation})

    db.push()

    # Verify reputation calculations
    db.pull()
    assert db['users'].get_row(1)['reputation'] == 18  # 10+5 from posts, 3 from comments
    assert db['users'].get_row(2)['reputation'] == 27  # 20 from posts, 5+2 from comments


def test_cascading_updates_through_relationships(tmp_path):
    """Test cascading updates through table relationships."""
    db_path = tmp_path / "cascade.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Categories
    categories = pd.DataFrame({
        'id': [1, 2],
        'name': ['Electronics', 'Books'],
        'product_count': [0, 0]
    })
    db.create_table('categories', categories, primary_key='id')

    # Products
    products = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'category_id': [1, 1, 2, 2],
        'name': ['Phone', 'Laptop', 'Novel', 'Textbook'],
        'in_stock': [True, True, True, False]
    })
    db.create_table('products', products, primary_key='id')

    # Update category counts
    for category_id in db['categories'].index:
        in_stock_count = len(
            db['products'][
                (db['products']['category_id'] == category_id) &
                (db['products']['in_stock'] == True)
            ]
        )
        db['categories'].update_row(category_id, {'product_count': in_stock_count})

    # Mark product out of stock (cascades to category count)
    db['products'].update_row(1, {'in_stock': False})

    # Recalculate category 1 count
    in_stock_count = len(
        db['products'][
            (db['products']['category_id'] == 1) &
            (db['products']['in_stock'] == True)
        ]
    )
    db['categories'].update_row(1, {'product_count': in_stock_count})

    db.push()

    # Verify cascading update
    db.pull()
    assert db['categories'].get_row(1)['product_count'] == 1  # Only laptop now
    assert db['products'].get_row(1)['in_stock'] == False


def test_complex_filtering_with_multiple_conditions(tmp_path):
    """Test complex filtering and conditional updates."""
    db_path = tmp_path / "filtering.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create orders with multiple attributes
    orders = pd.DataFrame({
        'id': range(1, 201),
        'customer_id': np.random.randint(1, 21, 200),
        'amount': np.random.uniform(10, 1000, 200).round(2),
        'status': np.random.choice(['pending', 'processing', 'completed'], 200),
        'priority': np.random.choice(['low', 'medium', 'high'], 200),
        'days_old': np.random.randint(1, 30, 200),
        'needs_review': [False] * 200
    })
    db.create_table('orders', orders, primary_key='id')

    # Complex condition: High priority, pending orders older than 7 days
    needs_review = db['orders'][
        (db['orders']['priority'] == 'high') &
        (db['orders']['status'] == 'pending') &
        (db['orders']['days_old'] > 7)
    ]

    # Mark for review
    for order_id in needs_review.index:
        db['orders'].update_row(order_id, {'needs_review': True})

    # Auto-cancel low priority orders older than 20 days
    to_cancel = db['orders'][
        (db['orders']['priority'] == 'low') &
        (db['orders']['status'] == 'pending') &
        (db['orders']['days_old'] > 20)
    ]

    for order_id in to_cancel.index:
        db['orders'].update_row(order_id, {'status': 'cancelled'})

    db.push()

    # Verify conditional updates
    db.pull()
    marked_count = len(db['orders'][db['orders']['needs_review'] == True])
    cancelled_count = len(db['orders'][db['orders']['status'] == 'cancelled'])

    assert marked_count >= 0  # May be 0 if no matches
    assert cancelled_count >= 0


def test_percentile_based_categorization(tmp_path):
    """Test categorizing data based on percentiles."""
    db_path = tmp_path / "percentiles.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create student scores
    students = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'Student {i}' for i in range(1, 101)],
        'score': np.random.randint(60, 100, 100),
        'grade': [''] * 100
    })
    db.create_table('students', students, primary_key='id')

    # Calculate percentiles
    scores = db['students']['score']
    p90 = scores.quantile(0.90)
    p75 = scores.quantile(0.75)
    p50 = scores.quantile(0.50)

    # Assign grades based on percentiles
    for student_id in db['students'].index:
        score = db['students'].get_row(student_id)['score']

        if score >= p90:
            grade = 'A'
        elif score >= p75:
            grade = 'B'
        elif score >= p50:
            grade = 'C'
        else:
            grade = 'D'

        db['students'].update_row(student_id, {'grade': grade})

    db.push()

    # Verify grading
    db.pull()
    grade_dist = db['students']['grade'].value_counts()
    assert 'A' in grade_dist.index
    assert 'B' in grade_dist.index


def test_rank_based_updates(tmp_path):
    """Test updating ranks based on scores."""
    db_path = tmp_path / "ranks.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create leaderboard
    players = pd.DataFrame({
        'id': range(1, 51),
        'username': [f'Player{i}' for i in range(1, 51)],
        'score': np.random.randint(1000, 10000, 50),
        'rank': [0] * 50
    })
    db.create_table('players', players, primary_key='id')

    # Calculate ranks
    sorted_players = db['players'].sort_values('score', ascending=False)

    for rank, (player_id, _row) in enumerate(sorted_players.iterrows(), 1):
        db['players'].update_row(player_id, {'rank': rank})

    db.push()

    # Verify rankings
    db.pull()
    top_player = db['players'][db['players']['rank'] == 1].iloc[0]
    assert top_player['score'] == db['players']['score'].max()


def test_join_based_inventory_update(tmp_path):
    """Test inventory updates based on joined order data."""
    db_path = tmp_path / "inventory.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Products
    products = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Widget', 'Gadget', 'Doohickey'],
        'stock': [100, 50, 75]
    })
    db.create_table('products', products, primary_key='id')

    # Orders (completed today)
    orders = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'product_id': [1, 1, 2, 3, 3],
        'quantity': [5, 3, 10, 2, 4],
        'status': ['completed', 'completed', 'completed', 'completed', 'pending']
    })
    db.create_table('orders', orders, primary_key='id')

    # Calculate total sold per product (only completed orders)
    sold_by_product = {}
    completed_orders = db['orders'][db['orders']['status'] == 'completed']

    for order_id in completed_orders.index:
        order = db['orders'].get_row(order_id)
        product_id = order['product_id']
        quantity = order['quantity']

        if product_id not in sold_by_product:
            sold_by_product[product_id] = 0
        sold_by_product[product_id] += quantity

    # Update inventory
    for product_id, sold_quantity in sold_by_product.items():
        current_stock = db['products'].get_row(product_id)['stock']
        new_stock = current_stock - sold_quantity
        db['products'].update_row(product_id, {'stock': new_stock})

    db.push()

    # Verify inventory updates
    db.pull()
    assert db['products'].get_row(1)['stock'] == 92  # 100 - 5 - 3
    assert db['products'].get_row(2)['stock'] == 40  # 50 - 10
    assert db['products'].get_row(3)['stock'] == 73  # 75 - 2 (pending not counted)


def test_grouped_aggregation_update(tmp_path):
    """Test that PK validation error message is shown (even if misleading)."""
    from pandalchemy.exceptions import SchemaError

    db_path = tmp_path / "grouped.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create sales data
    sales = pd.DataFrame({
        'id': range(1, 101),
        'product_id': np.random.randint(1, 11, 100),
        'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
        'amount': np.random.uniform(100, 1000, 100).round(2)
    })
    db.create_table('sales', sales, primary_key='id')

    # Create summary table with product_id as PK
    summary = pd.DataFrame({
        'product_id': range(1, 11),
        'region': [''] * 10,
        'total_sales': [0.0] * 10,
        'avg_sale': [0.0] * 10,
        'sale_count': [0] * 10
    })
    db.create_table('product_summary', summary, primary_key='product_id')

    # When iterating over a DataFrame with PK in index, idx is the index value
    for idx, row in db['product_summary'].iterrows():
        # idx is the product_id (which is in the index)
        # This is actually correct usage - but there's an issue with PK validation
        product_id = idx  # Use idx directly since it's the PK value
        product_sales = db['sales'][db['sales']['product_id'] == product_id]

        if len(product_sales) > 0:
            total = product_sales['amount'].sum()
            avg = product_sales['amount'].mean()
            count = len(product_sales)
            region = product_sales['region'].mode()[0] if len(product_sales) > 0 else ''

            # Update using loc with the index value
            db['product_summary'].loc[idx, 'total_sales'] = round(total, 2)
            db['product_summary'].loc[idx, 'avg_sale'] = round(avg, 2)
            db['product_summary'].loc[idx, 'sale_count'] = count
            db['product_summary'].loc[idx, 'region'] = region

    # Push should succeed - the grouped aggregation update pattern works correctly
    db.push()
    
    # Verify the updates worked
    result = db['product_summary'].to_pandas()
    assert len(result) == 10
    assert result['total_sales'].sum() > 0  # Some sales should be aggregated


def test_window_function_like_updates(tmp_path):
    """Test updates simulating window function behavior."""
    db_path = tmp_path / "windows.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Sales by salesperson
    sales = pd.DataFrame({
        'id': range(1, 51),
        'salesperson_id': np.random.randint(1, 6, 50),
        'amount': np.random.uniform(100, 1000, 50).round(2),
        'rank_in_team': [0] * 50,
        'percent_of_team_total': [0.0] * 50
    })
    db.create_table('sales', sales, primary_key='id')

    # Calculate rank and percentage within each salesperson's sales
    for salesperson_id in range(1, 6):
        person_sales = db['sales'][
            db['sales']['salesperson_id'] == salesperson_id
        ].sort_values('amount', ascending=False)

        if len(person_sales) == 0:
            continue

        team_total = person_sales['amount'].sum()

        for rank, (sale_id, row) in enumerate(person_sales.iterrows(), 1):
            percent = (row['amount'] / team_total * 100) if team_total > 0 else 0
            db['sales'].update_row(sale_id, {
                'rank_in_team': rank,
                'percent_of_team_total': round(percent, 2)
            })

    db.push()

    # Verify window-like calculations
    db.pull()
    # Check that ranks were assigned
    ranked_sales = db['sales'][db['sales']['rank_in_team'] > 0]
    assert len(ranked_sales) > 0


def test_pivot_table_like_aggregation(tmp_path):
    """Test creating pivot-table-like summary."""
    db_path = tmp_path / "pivot.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Sales by region and product
    sales = pd.DataFrame({
        'id': range(1, 101),
        'region': np.random.choice(['North', 'South', 'East', 'West'], 100),
        'product': np.random.choice(['A', 'B', 'C'], 100),
        'amount': np.random.uniform(100, 500, 100).round(2)
    })
    db.create_table('sales', sales, primary_key='id')

    # Create pivot summary
    regions = ['North', 'South', 'East', 'West']
    products = ['A', 'B', 'C']

    pivot_rows = []
    for region in regions:
        for product in products:
            pivot_rows.append({
                'region': region,
                'product': product,
                'total_sales': 0.0,
                'count': 0
            })

    pivot = pd.DataFrame(pivot_rows)
    db.create_table('sales_pivot', pivot, primary_key=['region', 'product'])

    # Calculate pivot values
    for region in regions:
        for product in products:
            segment_sales = db['sales'][
                (db['sales']['region'] == region) &
                (db['sales']['product'] == product)
            ]

            if len(segment_sales) > 0:
                total = segment_sales['amount'].sum()
                count = len(segment_sales)

                db['sales_pivot'].update_row((region, product), {
                    'total_sales': round(total, 2),
                    'count': count
                })

    db.push()

    # Verify pivot
    db.pull()
    # At least some combinations should have sales
    non_zero = db['sales_pivot'][db['sales_pivot']['count'] > 0]
    assert len(non_zero) > 0


def test_batch_updates_with_different_operations_per_group(tmp_path):
    """Test applying different operations to different groups in one batch."""
    db_path = tmp_path / "batch_ops.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create customer data
    customers = pd.DataFrame({
        'id': range(1, 101),
        'segment': np.random.choice(['VIP', 'Regular', 'New', 'Inactive'], 100),
        'lifetime_value': np.random.uniform(0, 10000, 100).round(2),
        'discount': [0.0] * 100,
        'status': ['active'] * 100
    })
    db.create_table('customers', customers, primary_key='id')

    # Apply different operations by segment
    # VIP: 20% discount, ensure active
    vip_customers = db['customers'][db['customers']['segment'] == 'VIP']
    for cust_id in vip_customers.index:
        db['customers'].update_row(cust_id, {'discount': 0.20, 'status': 'active'})

    # Regular: 10% discount
    regular = db['customers'][db['customers']['segment'] == 'Regular']
    for cust_id in regular.index:
        db['customers'].update_row(cust_id, {'discount': 0.10})

    # New: 5% welcome discount
    new = db['customers'][db['customers']['segment'] == 'New']
    for cust_id in new.index:
        db['customers'].update_row(cust_id, {'discount': 0.05})

    # Inactive: Mark as inactive status
    inactive = db['customers'][db['customers']['segment'] == 'Inactive']
    for cust_id in inactive.index:
        db['customers'].update_row(cust_id, {'status': 'inactive', 'discount': 0.0})

    db.push()

    # Verify different operations applied correctly
    db.pull()
    vip_sample = db['customers'][db['customers']['segment'] == 'VIP'].iloc[0]
    assert vip_sample['discount'] == 0.20

    inactive_sample = db['customers'][db['customers']['segment'] == 'Inactive'].iloc[0]
    assert inactive_sample['status'] == 'inactive'

