# test_collect.py
import pandas as pd
from collect import save_to_csv, scrape_volcano_data

def test_scrape_volcano_data_with_real_url():
    """
    Integration test using a real URL from the Smithsonian Global Volcanism Program.
    Note: This makes an actual HTTP request (use sparingly to avoid overloading servers).
    """
    # Use a recent date in the correct format (YYYY-MM-DD)
    test_date = "2024-06-15"  # Example date within the knowledge cutoff

    # Call the function with the real URL
    eruption_df, unrest_df = scrape_volcano_data(test_date)

    # Basic assertions (verify we got DataFrames back)
    assert isinstance(eruption_df, (pd.DataFrame, type(None))), "Eruption data should be a DataFrame or None"
    assert isinstance(unrest_df, (pd.DataFrame, type(None))), "Unrest data should be a DataFrame or None"

    # If data exists, verify structure
    if eruption_df is not None:
        assert not eruption_df.empty, "Eruption DataFrame should not be empty if data exists"
        assert "Name" in eruption_df.columns, "Eruption DataFrame should have 'Name' column"

    if unrest_df is not None:
        assert not unrest_df.empty, "Unrest DataFrame should not be empty if data exists"
        assert "Name" in unrest_df.columns, "Unrest DataFrame should have 'Name' column"

def test_save_to_csv(tmp_path):
    """Simple test for save_to_csv function using pytest"""
    # 1. Create test DataFrame
    test_df = pd.DataFrame({
        'Name': ['Test Volcano'],
        'Country': ['Testland']
    })

    # 2. Create file path in temporary directory
    output_file = tmp_path / "test_output.csv"

    # 3. Call the function
    save_to_csv(test_df, str(output_file), "test")

    # 4. Verify file exists and contains correct data
    assert output_file.exists()

    # 5. Verify content
    saved_df = pd.read_csv(output_file)
    pd.testing.assert_frame_equal(saved_df, test_df)
