import pandas as pd
import mmh3
from kazoo.client import KazooClient

def read_csv(file_path):
    """Read the CSV file into a DataFrame."""
    return pd.read_csv(file_path)

def generate_murmurhash(row):
    """Generate a MurmurHash for a row."""
    row_string = str(row.values)
    row_encoded = row_string.encode('utf-8')
    hash_value = mmh3.hash(row_encoded)
    return hash_value

def add_murmurhash_column(df):
    """Add a MurmurHash column to the DataFrame."""
    df['murmurhash'] = df.apply(generate_murmurhash, axis=1)
    return df

def main(file_path):
    """Main function to read the CSV and add MurmurHash for each row."""
    # Read the CSV file
    df = read_csv(file_path)
    
    # Add the MurmurHash column
    df = add_murmurhash_column(df)
    
    # Display the DataFrame with the new hash column
    print(df.head())


def mainzk():
    try:
        # Connect to ZooKeeper
        zk = KazooClient(hosts='127.0.0.1:2181')
        zk.start()

        # Create a ZNode
        try:
            zk.create("/my/path", b"a value")
            print("Created /my/path")
        except Exception as e:
            print(f"Error creating /my/path: {e}")

        # Check if ZNode exists
        if zk.exists("/my/path"):
            print("/my/path exists")
        else:
            print("/my/path does not exist")

        # Get data from ZNode
        try:
            data, stat = zk.get("/my/path")
            print("Data:", data.decode("utf-8"))
            print("Version:", stat.version)
        except NoNodeError:
            print("/my/path does not exist when trying to get data")

        # Set data for ZNode
        try:
            zk.set("/my/path", b"some data")
            print("Set data for /my/path")
        except NoNodeError:
            print("/my/path does not exist when trying to set data")

        # Watch ZNode
        def watch_node(event):
            print(f"Watch triggered: {event}")

        # Set a watch on /my/path
        zk.DataWatch("/my/path", watch_node)

        # Trigger the watch by setting data again
        zk.set("/my/path", b"new data")

        # Create an ephemeral ZNode
        try:
            zk.create("/my/ephemeral", b"ephemeral data", ephemeral=True)
            print("Created /my/ephemeral")
        except Exception as e:
            print(f"Error creating /my/ephemeral: {e}")

        # Create a sequential ZNode
        try:
            sequential_path = zk.create("/my/sequential-", b"sequential data", sequence=True)
            print(f"Created {sequential_path}")
        except Exception as e:
            print(f"Error creating sequential ZNode: {e}")

        # Delete a ZNode
        try:
            zk.delete("/my/path")
            print("Deleted /my/path")
        except NoNodeError:
            print("/my/path does not exist when trying to delete")

    finally:
        # Close the connection
        zk.stop()
        zk.close()
        print("ZooKeeper connection closed")




if __name__ == "__main__":
    # Specify the path to your CSV file
    csv_file_path = 'sample.csv'
    main(csv_file_path)

    # mainzk()
    # zk = KazooClient(hosts='127.0.0.1:2181')
    # zk.start()
    
    # # Call my_func when the children change
    # children = zk.get_children("/zk/", watch=my_func)