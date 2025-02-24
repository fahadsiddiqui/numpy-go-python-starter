import os
import numpy as np
import pandas as pd

# List of NPZ filenames (without the folder prefix).
npz_files = [
    "actions.npz",
    "agent_key_values.npz",
    "agents.npz",
    "api_keys.npz",
    "execution_plans.npz",
    "memory.npz",
    "memory_document.npz",
    "notifications.npz",
    "prompts.npz",
    "saga_events.npz",
    "sagas.npz",
    "schemas.npz",
    "tool_configurations.npz",
    "toolboxes.npz",
    "tools.npz",
    "tools_to_agents.npz",
    "tools_to_schemas.npz",
    "triggers.npz",
    "user_sessions.npz",
    "users.npz",
    "world_model.npz",
    "world_model_canvas_relations.npz",
    "world_model_data_source_configuration.npz",
]

folder = "data"

for file_name in npz_files:
    file_path = os.path.join(folder, file_name)
    try:
        # Load the NPZ file. Using a context manager ensures proper cleanup.
        with np.load(file_path) as npz_data:
            # Create a dictionary from the NPZ file.
            # npz_data.files gives you the list of keys stored in the file.
            data_dict = {key: npz_data[key] for key in npz_data.files}
            
            # Create a DataFrame from the dictionary.
            df = pd.DataFrame(data_dict)
    except Exception as e:
        print(f"Error loading {file_name}: {e}")
        continue

    print(f"\n--- NPZ File: {file_name} ---")
    # Display the first few rows and the data types.
    print(df.head())
    print("\nData Types:")
    print(df.dtypes)
    print("=" * 50)