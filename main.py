from CustomerDataExtractor import CustomerDataExtractor

# Define file paths
pickle_path = r'C:\Users\PC2\Desktop\Internship_Exam\customer_orders.pkl'           # Replace with actual path to your pickle file
vip_path = r'C:\Users\PC2\Desktop\Internship_Exam\vip_customers.txt'              # Replace with actual path to your VIP customer IDs
output_csv = 'customer_items.csv'    # Desired output CSV file

# Create an instance of the extractor
extractor = CustomerDataExtractor(pickle_path, vip_path, output_csv)

# Run the transformation and export
df = extractor.transform()

# Optional: print the first few rows to verify
print(df.head())