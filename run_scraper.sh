#!/bin/bash
# ============================================================
# Amazon Egypt Scraper - Manual Trigger Script
# Run this file to trigger the scraper on GitHub Actions
# Usage: bash run_scraper.sh
# ============================================================

# Set your GitHub token here or export it as environment variable before running
# export GH_TOKEN="your_token_here"
REPO="mostafa11100/amazon-egypt-scraper"
GH_CLI="/c/Program Files/GitHub CLI/gh.exe"

echo "🚀 Triggering Amazon Egypt Scraper on GitHub Actions..."
echo ""

GH_TOKEN="$GH_TOKEN" "$GH_CLI" workflow run scrape.yml --repo "$REPO"

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Scraper triggered successfully!"
    echo "📊 Watch progress at: https://github.com/$REPO/actions"
    echo ""
    echo "⏳ The scraper will run on GitHub servers."
    echo "📁 Results will be saved to: products_full_data.json in the repo"
else
    echo "❌ Failed to trigger scraper. Check your token or internet connection."
fi
