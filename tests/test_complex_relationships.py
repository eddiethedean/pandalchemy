"""Tests for complex multi-table relationships."""

import pandas as pd
from sqlalchemy import create_engine

from pandalchemy import DataBase


def test_hierarchical_categories_tree(tmp_path):
    """Test hierarchical category tree with self-reference."""
    db_path = tmp_path / "hierarchy.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create categories with parent_id self-reference
    categories = pd.DataFrame({
        'id': [1, 2, 3, 4, 5, 6],
        'name': ['Electronics', 'Computers', 'Laptops', 'Desktops', 'Phones', 'Smartphones'],
        'parent_id': [None, 1, 2, 2, 1, 5]  # Electronics is root
    })
    db.create_table('categories', categories, primary_key='id')

    # Add new subcategory
    db['categories'].add_row({
        'id': 7,
        'name': 'Gaming Laptops',
        'parent_id': 3  # Under Laptops
    })

    # Move category to different parent
    db['categories'].update_row(6, {'parent_id': 1})  # Move Smartphones under Electronics

    # Delete category (would need cascade in real app)
    db['categories'].delete_row(4)  # Delete Desktops

    db.push()

    # Verify hierarchy
    db.pull()
    assert db['categories'].get_row(7)['parent_id'] == 3
    assert db['categories'].get_row(6)['parent_id'] == 1
    assert not db['categories'].row_exists(4)


def test_many_to_many_with_enrollment_attributes(tmp_path):
    """Test student-course enrollment with grades and status."""
    db_path = tmp_path / "school.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Create students
    students = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'gpa': [0.0, 0.0, 0.0, 0.0, 0.0]
    })
    db.create_table('students', students, primary_key='id')

    # Create courses
    courses = pd.DataFrame({
        'id': [101, 102, 103],
        'name': ['Math 101', 'CS 201', 'Physics 101'],
        'credits': [3, 4, 3]
    })
    db.create_table('courses', courses, primary_key='id')

    # Create enrollments with composite PK
    enrollments = pd.DataFrame({
        'student_id': [1, 1, 2, 2, 3],
        'course_id': [101, 102, 101, 103, 102],
        'grade': [None, None, 'A', 'B', None],
        'status': ['enrolled', 'enrolled', 'completed', 'completed', 'enrolled']
    })
    db.create_table('enrollments', enrollments, primary_key=['student_id', 'course_id'])

    # Enroll new student in course
    db['enrollments'].add_row({
        'student_id': 4,
        'course_id': 101,
        'grade': None,
        'status': 'enrolled'
    })

    # Update grade for completed course
    db['enrollments'].update_row((1, 101), {'grade': 'A', 'status': 'completed'})
    db['enrollments'].update_row((1, 102), {'grade': 'B+', 'status': 'completed'})

    # Drop course
    db['enrollments'].delete_row((3, 102))

    db.push()

    # Calculate GPA (simplified: A=4, B=3, etc.)
    grade_points = {'A': 4.0, 'A-': 3.7, 'B+': 3.3, 'B': 3.0, 'B-': 2.7, 'C': 2.0}

    # Get completed enrollments for student 1
    db.pull()
    student_1_enrollments = db['enrollments'][
        (db['enrollments'].index.get_level_values('student_id') == 1) &
        (db['enrollments']['status'] == 'completed')
    ]

    total_points = 0
    total_credits = 0
    for idx, enrollment in student_1_enrollments.iterrows():
        course_id = idx[1]
        grade = enrollment['grade']
        if grade in grade_points:
            credits = db['courses'].get_row(course_id)['credits']
            total_points += grade_points[grade] * credits
            total_credits += credits

    if total_credits > 0:
        gpa = total_points / total_credits
        db['students'].update_row(1, {'gpa': round(gpa, 2)})

    db.push()

    # Verify
    db.pull()
    assert db['students'].get_row(1)['gpa'] > 0
    assert db['enrollments'].row_exists((4, 101))
    assert not db['enrollments'].row_exists((3, 102))


def test_bulk_enrollment_semester_registration(tmp_path):
    """Test bulk enrollment for semester registration."""
    db_path = tmp_path / "school.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    students = pd.DataFrame({
        'id': range(1, 51),  # 50 students
        'name': [f'Student {i}' for i in range(1, 51)]
    })
    db.create_table('students', students, primary_key='id')

    courses = pd.DataFrame({
        'id': [101, 102, 103, 104, 105],
        'name': ['Math', 'CS', 'Physics', 'Chemistry', 'Biology'],
        'max_students': [30, 25, 30, 20, 25]
    })
    db.create_table('courses', courses, primary_key='id')

    enrollments = pd.DataFrame({
        'student_id': [],
        'course_id': [],
        'status': []
    })
    db.create_table('enrollments', enrollments, primary_key=['student_id', 'course_id'])

    # Bulk enroll students
    # Students 1-30 in Math (101)
    # Students 1-25 in CS (102)
    # Students 26-50 in Physics (103)
    enrollments_to_add = []

    for student_id in range(1, 31):
        enrollments_to_add.append({
            'student_id': student_id,
            'course_id': 101,
            'status': 'enrolled'
        })

    for student_id in range(1, 26):
        enrollments_to_add.append({
            'student_id': student_id,
            'course_id': 102,
            'status': 'enrolled'
        })

    for student_id in range(26, 51):
        enrollments_to_add.append({
            'student_id': student_id,
            'course_id': 103,
            'status': 'enrolled'
        })

    db['enrollments'].bulk_insert(enrollments_to_add)
    db.push()

    # Verify enrollment counts
    db.pull()
    total_enrollments = len(db['enrollments'])
    assert total_enrollments == 80  # 30 + 25 + 25

    # Verify specific enrollments
    assert db['enrollments'].row_exists((1, 101))
    assert db['enrollments'].row_exists((25, 102))
    assert db['enrollments'].row_exists((50, 103))


def test_multi_level_organization_hierarchy(tmp_path):
    """Test multi-level organization hierarchy with employees."""
    db_path = tmp_path / "org.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Departments
    departments = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'name': ['Engineering', 'Sales', 'Marketing', 'HR'],
        'budget': [1000000, 500000, 300000, 200000]
    })
    db.create_table('departments', departments, primary_key='id')

    # Teams (belong to departments)
    teams = pd.DataFrame({
        'id': [10, 11, 12, 13],
        'name': ['Backend', 'Frontend', 'Enterprise', 'Retail'],
        'department_id': [1, 1, 2, 2]
    })
    db.create_table('teams', teams, primary_key='id')

    # Employees
    employees = pd.DataFrame({
        'id': [100, 101, 102, 103, 104],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'team_id': [10, 10, 11, 12, 13],
        'salary': [100000, 95000, 90000, 85000, 80000]
    })
    db.create_table('employees', employees, primary_key='id')

    # Projects
    projects = pd.DataFrame({
        'id': [1, 2],
        'name': ['Project Alpha', 'Project Beta'],
        'team_id': [10, 11],
        'budget': [50000, 75000]
    })
    db.create_table('projects', projects, primary_key='id')

    # Employee assignments (composite PK)
    assignments = pd.DataFrame({
        'employee_id': [100, 101, 102],
        'project_id': [1, 1, 2],
        'role': ['lead', 'developer', 'lead'],
        'hours_allocated': [40, 40, 40]
    })
    db.create_table('assignments', assignments, primary_key=['employee_id', 'project_id'])

    # Reorganization: Move Backend team to different department
    db['teams'].update_row(10, {'department_id': 2})  # Engineering -> Sales

    # Promote employee
    db['employees'].update_row(101, {'salary': 105000})

    # Reassign employee to different project
    db['assignments'].delete_row((101, 1))
    db['assignments'].add_row({
        'employee_id': 101,
        'project_id': 2,
        'role': 'developer',
        'hours_allocated': 40
    })

    # Add new employee and assign to project
    db['employees'].add_row({
        'id': 105,
        'name': 'Frank',
        'team_id': 10,
        'salary': 92000
    })
    db['assignments'].add_row({
        'employee_id': 105,
        'project_id': 1,
        'role': 'developer',
        'hours_allocated': 40
    })

    db.push()

    # Verify multi-level consistency
    db.pull()
    assert db['teams'].get_row(10)['department_id'] == 2
    assert db['employees'].get_row(101)['salary'] == 105000
    assert not db['assignments'].row_exists((101, 1))
    assert db['assignments'].row_exists((101, 2))
    assert db['employees'].row_exists(105)


def test_circular_reference_tables(tmp_path):
    """Test tables with circular references (A->B, B->A)."""
    db_path = tmp_path / "circular.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Countries and cities with circular references
    # Country has capital_city_id, City has country_id

    countries = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['USA', 'France', 'Japan'],
        'capital_city_id': [None, None, None]  # Will be updated
    })
    db.create_table('countries', countries, primary_key='id')

    cities = pd.DataFrame({
        'id': [10, 20, 30],
        'name': ['Washington DC', 'Paris', 'Tokyo'],
        'country_id': [1, 2, 3],
        'population': [700000, 2200000, 14000000]
    })
    db.create_table('cities', cities, primary_key='id')

    # Now update countries with capital city references
    db['countries'].update_row(1, {'capital_city_id': 10})
    db['countries'].update_row(2, {'capital_city_id': 20})
    db['countries'].update_row(3, {'capital_city_id': 30})

    # Add new country and city
    db['countries'].add_row({
        'id': 4,
        'name': 'Germany',
        'capital_city_id': None
    })
    db['cities'].add_row({
        'id': 40,
        'name': 'Berlin',
        'country_id': 4,
        'population': 3700000
    })
    db['countries'].update_row(4, {'capital_city_id': 40})

    db.push()

    # Verify circular references maintained
    db.pull()
    assert db['countries'].get_row(1)['capital_city_id'] == 10
    assert db['cities'].get_row(10)['country_id'] == 1
    assert db['countries'].get_row(4)['capital_city_id'] == 40
    assert db['cities'].get_row(40)['country_id'] == 4


def test_multi_level_join_updates(tmp_path):
    """Test updates affecting multiple levels of joined tables."""
    db_path = tmp_path / "multi.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # 5-level hierarchy: Company -> Department -> Team -> Employee -> TimeEntry

    companies = pd.DataFrame({
        'id': [1],
        'name': ['Acme Corp'],
        'revenue': [10000000.0]
    })
    db.create_table('companies', companies, primary_key='id')

    departments = pd.DataFrame({
        'id': [1, 2],
        'company_id': [1, 1],
        'name': ['Engineering', 'Sales'],
        'budget': [1000000.0, 500000.0]
    })
    db.create_table('departments', departments, primary_key='id')

    teams = pd.DataFrame({
        'id': [10, 11],
        'department_id': [1, 2],
        'name': ['Backend Team', 'Enterprise Team'],
        'size': [5, 3]
    })
    db.create_table('teams', teams, primary_key='id')

    employees = pd.DataFrame({
        'id': [100, 101, 102],
        'team_id': [10, 10, 11],
        'name': ['Alice', 'Bob', 'Charlie'],
        'hourly_rate': [50.0, 45.0, 55.0]
    })
    db.create_table('employees', employees, primary_key='id')

    time_entries = pd.DataFrame({
        'id': [1, 2, 3, 4],
        'employee_id': [100, 100, 101, 102],
        'hours': [8, 8, 8, 8],
        'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-01'],
        'billable': [True, True, True, True]
    })
    db.create_table('time_entries', time_entries, primary_key='id')

    # Complex update: Company-wide salary increase
    # Increase all employee rates by 10%
    for employee_id in db['employees'].index:
        current_rate = db['employees'].get_row(employee_id)['hourly_rate']
        db['employees'].update_row(employee_id, {'hourly_rate': current_rate * 1.1})

    # Calculate total labor cost per team
    team_costs = {}
    for time_entry_id in db['time_entries'].index:
        entry = db['time_entries'].get_row(time_entry_id)
        employee = db['employees'].get_row(entry['employee_id'])
        team_id = employee['team_id']

        cost = entry['hours'] * employee['hourly_rate']
        if team_id not in team_costs:
            team_costs[team_id] = 0
        team_costs[team_id] += cost

    # Update department budgets based on team costs
    for team_id, cost in team_costs.items():
        team = db['teams'].get_row(team_id)
        dept_id = team['department_id']
        current_budget = db['departments'].get_row(dept_id)['budget']
        # Deduct costs from budget
        db['departments'].update_row(dept_id, {'budget': current_budget - cost})

    db.push()

    # Verify cascading updates
    db.pull()
    assert db['employees'].get_row(100)['hourly_rate'] == 50.0 * 1.1
    assert db['employees'].get_row(101)['hourly_rate'] == 45.0 * 1.1
    # Budgets should be reduced
    assert db['departments'].get_row(1)['budget'] < 1000000.0


def test_social_network_relationships(tmp_path):
    """Test social network with followers (many-to-many self-reference)."""
    db_path = tmp_path / "social.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Users
    users = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'username': ['alice', 'bob', 'charlie', 'diana', 'eve'],
        'follower_count': [0, 0, 0, 0, 0],
        'following_count': [0, 0, 0, 0, 0]
    })
    db.create_table('users', users, primary_key='id')

    # Followers (composite PK: follower_id, followed_id)
    followers = pd.DataFrame({
        'follower_id': [1, 1, 2, 3],
        'followed_id': [2, 3, 1, 1],
        'followed_at': [str(pd.Timestamp.now())] * 4
    })
    db.create_table('followers', followers, primary_key=['follower_id', 'followed_id'])

    # User 1 follows user 4
    db['followers'].add_row({
        'follower_id': 1,
        'followed_id': 4,
        'followed_at': str(pd.Timestamp.now())
    })

    # User 5 follows users 1, 2, 3
    new_follows = [
        {'follower_id': 5, 'followed_id': 1, 'followed_at': str(pd.Timestamp.now())},
        {'follower_id': 5, 'followed_id': 2, 'followed_at': str(pd.Timestamp.now())},
        {'follower_id': 5, 'followed_id': 3, 'followed_at': str(pd.Timestamp.now())}
    ]
    db['followers'].bulk_insert(new_follows)

    # Update follower/following counts
    # Count followers for each user
    for user_id in db['users'].index:
        # Count who follows this user
        follower_count = len(
            db['followers'][
                db['followers'].index.get_level_values('followed_id') == user_id
            ]
        )
        # Count who this user follows
        following_count = len(
            db['followers'][
                db['followers'].index.get_level_values('follower_id') == user_id
            ]
        )
        db['users'].update_row(user_id, {
            'follower_count': follower_count,
            'following_count': following_count
        })

    db.push()

    # Verify counts
    db.pull()
    assert db['users'].get_row(1)['follower_count'] == 3  # Followed by 2, 3, 5
    assert db['users'].get_row(1)['following_count'] == 3  # Follows 2, 3, 4
    assert db['users'].get_row(5)['following_count'] == 3


def test_product_variant_hierarchy(tmp_path):
    """Test product variants with hierarchical relationships."""
    db_path = tmp_path / "products.db"
    engine = create_engine(f"sqlite:///{db_path}")
    db = DataBase(engine)

    # Base products
    products = pd.DataFrame({
        'id': [1, 2],
        'name': ['T-Shirt', 'Jeans'],
        'base_price': [19.99, 49.99]
    })
    db.create_table('products', products, primary_key='id')

    # Variants (composite PK: product_id, size, color)
    variants = pd.DataFrame({
        'product_id': [1, 1, 1, 2, 2],
        'size': ['S', 'M', 'L', '32', '34'],
        'color': ['Red', 'Red', 'Blue', 'Blue', 'Black'],
        'sku': ['TS-S-R', 'TS-M-R', 'TS-L-B', 'J-32-B', 'J-34-BK'],
        'stock': [10, 15, 8, 5, 12],
        'price_adjustment': [0.0, 0.0, 2.0, 0.0, 5.0]
    })
    db.create_table('variants', variants, primary_key=['product_id', 'size', 'color'])

    # Add new variant
    db['variants'].add_row({
        'product_id': 1,
        'size': 'XL',
        'color': 'Red',
        'sku': 'TS-XL-R',
        'stock': 20,
        'price_adjustment': 3.0
    })

    # Update stock after sale
    db['variants'].update_row((1, 'M', 'Red'), {'stock': 10})  # Sold 5

    # Discontinue variant
    db['variants'].delete_row((2, '32', 'Blue'))

    # Price increase for all Large sizes
    large_variants = db['variants'][
        db['variants'].index.get_level_values('size') == 'L'
    ]
    for idx in large_variants.index:
        current_adj = db['variants'].get_row(idx)['price_adjustment']
        db['variants'].update_row(idx, {'price_adjustment': current_adj + 2.0})

    db.push()

    # Verify
    db.pull()
    assert db['variants'].row_exists((1, 'XL', 'Red'))
    assert db['variants'].get_row((1, 'M', 'Red'))['stock'] == 10
    assert not db['variants'].row_exists((2, '32', 'Blue'))
    assert db['variants'].get_row((1, 'L', 'Blue'))['price_adjustment'] == 4.0  # 2.0 + 2.0

