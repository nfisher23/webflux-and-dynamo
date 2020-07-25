PHONE_TEMPLATE=$(cat <<'EOF'
{
    "company": "Nokia",
    "model": "1998 dumb phone",
    "colors": [
        "Red",
        "Silver"
    ],
    "size": 19
}
EOF
)

NOKIA=$(printf "$PHONE_TEMPLATE" "Nokia" "1999 dumb phone" "19")

curl -v -XPUT localhost:8080/phone -H "Content-Type: application/json" --data "$NOKIA"

