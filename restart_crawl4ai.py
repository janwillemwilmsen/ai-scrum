#!/usr/bin/env python3
"""
Helper script to restart Crawl4AI server when it crashes due to recursion bug.
This can be run automatically or manually when the server becomes unresponsive.
"""

import subprocess
import time
import requests
import sys

def check_server_health():
    """Check if the Crawl4AI server is responsive"""
    try:
        response = requests.get('http://localhost:11235/health', timeout=5)
        return response.status_code == 200
    except:
        return False

def restart_docker_container(container_name="crawl4ai"):
    """Restart the Crawl4AI Docker container"""
    try:
        print(f"🔄 Restarting Docker container '{container_name}'...")
        result = subprocess.run(['docker', 'restart', container_name], 
                              capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"✅ Container '{container_name}' restarted successfully")
            return True
        else:
            print(f"❌ Failed to restart container: {result.stderr}")
            return False
    except subprocess.TimeoutExpired:
        print("❌ Timeout while restarting container")
        return False
    except FileNotFoundError:
        print("❌ Docker command not found. Please install Docker or restart manually.")
        return False
    except Exception as e:
        print(f"❌ Error restarting container: {e}")
        return False

def wait_for_server_startup(max_wait=60):
    """Wait for the server to become responsive after restart"""
    print("⏳ Waiting for server to start up...")
    
    for i in range(max_wait):
        if check_server_health():
            print("✅ Server is responsive!")
            return True
        
        if i % 10 == 0:  # Print every 10 seconds
            print(f"⏱️  Still waiting... ({i}/{max_wait}s)")
        
        time.sleep(1)
    
    print(f"❌ Server did not become responsive within {max_wait} seconds")
    return False

def main():
    print("🚨 Crawl4AI Server Restart Tool")
    print("This tool restarts the Crawl4AI server to fix the recursion bug.")
    
    # Check current status
    if check_server_health():
        print("✅ Server is currently responsive")
        response = input("Do you still want to restart it? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print("👋 No restart needed. Exiting.")
            return
    else:
        print("❌ Server is not responsive - restart needed")
    
    # Try different common container names
    container_names = ["crawl4ai", "crawl4ai-server", "crawl4ai_server"]
    
    for container_name in container_names:
        print(f"\n🔍 Trying container name: '{container_name}'")
        if restart_docker_container(container_name):
            if wait_for_server_startup():
                print(f"\n🎉 Successfully restarted Crawl4AI server!")
                print("You can now continue running your scraper.")
                return
            else:
                print(f"⚠️ Container restarted but server not responsive")
        else:
            print(f"⚠️ Could not restart container '{container_name}'")
    
    print("\n❌ Could not automatically restart the server.")
    print("Please restart manually with one of these commands:")
    print("  docker restart crawl4ai")
    print("  docker restart crawl4ai-server") 
    print("  docker-compose restart")

if __name__ == "__main__":
    main() 