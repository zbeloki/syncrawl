from web_gui import create_app

def main():
    app = create_app()
    app.run(debug=True)  # Add debug=True for development
    
if __name__ == "__main__":
    main()
    
