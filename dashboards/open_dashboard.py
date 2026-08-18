"""
Launch the AWS QuickSight Executive Sales Dashboard in your default web browser.
"""

import os
import webbrowser

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dashboard_html_path = os.path.join(current_dir, "sales_dashboard.html")
    
    print(f"🚀 Launching Executive Sales Dashboard: {dashboard_html_path}")
    webbrowser.open(f"file://{dashboard_html_path}")
