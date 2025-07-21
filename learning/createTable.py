import pymysql
import json
import os

def load_schema(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def connect_db():
    return pymysql.connect(
        host='127.0.0.1',
        user='dbuser2',
        password='1234',
        database='housing',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    # "host":"127.0.0.1",
    # "user":"dbuser2",
    # "password":"1234",
    # "autocommit":True,  # 自動提交事務
    # "port":3306,  # 如果需要指定端口
    # "charset":'utf8mb4',  # 如果需要指定字符集
    # "database":"housing"        
    )

def get_existing_columns(cursor, table_name):
    cursor.execute("""
        SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA, COLUMN_COMMENT
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
    """, (table_name,))
    return {row['COLUMN_NAME']: row for row in cursor.fetchall()}

def build_column_sql(name, props):
    parts = [f"`{name}`", props['type']]
    if not props.get('nullable', True):
        parts.append("NOT NULL")
    if props.get('auto_increment'):
        parts.append("AUTO_INCREMENT")
    if props.get('comment'):
        parts.append(f"COMMENT '{props['comment']}'")
    return ' '.join(parts)

def create_table(cursor, table_name, columns):
    col_defs = []
    primary_keys = []

    for name, props in columns.items():
        col_defs.append(build_column_sql(name, props))
        if props.get('primary_key'):
            primary_keys.append(f"`{name}`")

    if primary_keys:
        col_defs.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    sql = f"CREATE TABLE `{table_name}` ({', '.join(col_defs)}) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
    cursor.execute(sql)
    print(f"✅ Created table `{table_name}`")

def alter_table(cursor, table_name, columns, existing_columns):
    for name, props in columns.items():
        if name not in existing_columns:
            # 新增欄位
            sql = f"ALTER TABLE `{table_name}` ADD COLUMN {build_column_sql(name, props)};"
            cursor.execute(sql)
            print(f"➕ Added column `{name}`")
        else:
            existing = existing_columns[name]
            expected_type = props['type'].lower()
            actual_type = existing['COLUMN_TYPE'].lower()
            expected_nullable = 'YES' if props.get('nullable', True) else 'NO'
            actual_nullable = existing['IS_NULLABLE']
            expected_comment = props.get('comment', '')
            actual_comment = existing.get('COLUMN_COMMENT', '')

            if expected_type != actual_type or expected_nullable != actual_nullable or expected_comment != actual_comment:
                sql = f"ALTER TABLE `{table_name}` MODIFY COLUMN {build_column_sql(name, props)};"
                cursor.execute(sql)
                print(f"✏️ Modified column `{name}`")

def sync_table(schema_file):
    schema = load_schema(schema_file)
    table_name = schema['table_name']
    columns = schema['columns']

    conn = connect_db()
    with conn.cursor() as cursor:
        # 檢查資料表是否存在
        cursor.execute("""
            SELECT COUNT(*) AS count 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
        """, (table_name,))
        exists = cursor.fetchone()['count'] > 0

        if not exists:
            create_table(cursor, table_name, columns)
        else:
            existing_columns = get_existing_columns(cursor, table_name)
            alter_table(cursor, table_name, columns, existing_columns)

        conn.commit()
    conn.close()

# ✅ 執行
if __name__ == '__main__':
    path = os.path.dirname(__file__)
    path = os.path.join(path,'building_transfer_schema.json')
    print(f"path:{path}")
    sync_table(path)
