import os
import numpy as np
import pandas as pd
import shutil

# List of table names that have NPY files in separate directories
table_names = [
    "actions",
    "agent_key_values",
    "agents",
    "api_keys",
    "execution_plans",
    "memory",
    "memory_document",
    "notifications",
    "prompts",
    "saga_events",
    "sagas",
    "schemas",
    "tool_configurations",
    "toolboxes",
    "tools",
    "tools_to_agents",
    "tools_to_schemas",
    "triggers",
    "user_sessions",
    "users",
    "world_model",
    "world_model_canvas_relations",
    "world_model_data_source_configuration",
]

base_folder = "data"

# PART 1: Convert NPY files to NPZ files
print("Converting NPY files to NPZ files...")

for table_name in table_names:
    table_folder = os.path.join(base_folder, table_name)
    
    # Check if the table folder exists
    if not os.path.exists(table_folder):
        print(f"Folder not found for table: {table_name}")
        continue
    
    # Get all .npy files in the table folder
    npy_files = [f for f in os.listdir(table_folder) if f.endswith('.npy')]
    
    if not npy_files:
        print(f"No .npy files found for table: {table_name}")
        continue
    
    # Dictionary to store arrays for NPZ
    arrays = {}
    
    # Process each NPY file
    for npy_file in npy_files:
        column_name = os.path.splitext(npy_file)[0]  # Remove .npy extension
        file_path = os.path.join(table_folder, npy_file)
        
        try:
            # Load the NPY file
            array_data = np.load(file_path, allow_pickle=True)
            arrays[column_name] = array_data
        except Exception as e:
            print(f"Error loading {npy_file}: {e}")
    
    # Save as NPZ if we have data
    if arrays:
        npz_path = os.path.join(base_folder, f"{table_name}.npz")
        np.savez(npz_path, **arrays)
        print(f"Created NPZ file: {npz_path}")
        
        # Optionally remove the original NPY directory
        # Uncomment the following line if you want to remove the directory
        # shutil.rmtree(table_folder)
    else:
        print(f"No data to save for table: {table_name}")

print("\nConversion complete.\n")

# PART 2: Read and display NPZ files (similar to your original code)
print("Reading NPZ files and displaying data...\n")

npz_files = [f"{name}.npz" for name in table_names]

for file_name in npz_files:
    file_path = os.path.join(base_folder, file_name)
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        continue
        
    try:
        # Load the NPZ file using a context manager
        with np.load(file_path) as npz_data:
            # Create a dictionary from the NPZ file
            data_dict = {key: npz_data[key] for key in npz_data.files}
            
            # Create a DataFrame from the dictionary
            df = pd.DataFrame(data_dict)
    except Exception as e:
        print(f"Error loading {file_name}: {e}")
        continue

    print(f"\n--- NPZ File: {file_name} ---")
    # Display the first few rows and the data types
    print(df.head())
    print("\nData Types:")
    print(df.dtypes)
    print("=" * 50)
