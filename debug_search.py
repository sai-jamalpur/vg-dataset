from duckduckgo_search import DDGS
import json

def test_search():
    ddgs = DDGS()
    query = "Animal Behavior animal alertness"
    print(f"Searching for videos: {query}")
    
    try:
        results = list(ddgs.videos(query, max_results=1))
        if results:
            print("Keys in result:", results[0].keys())
            print("First result:", results[0])
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_search()
