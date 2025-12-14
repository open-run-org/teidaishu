#!/usr/bin/env bash
set -euo pipefail

yaml_get() {
  local file="$1"
  local key="$2"
  awk -v key="$key" '
    function trim(s){ sub(/^[ \t]+/,"",s); sub(/[ \t]+$/,"",s); return s }
    function unquote(s){
      s=trim(s)
      gsub(/^"/,"",s); gsub(/"$/,"",s)
      gsub(/^'\''/,"",s); gsub(/'\''$/,"",s)
      return s
    }
    {
      line=$0
      sub(/\r$/,"",line)
      if(line ~ /^[[:space:]]*#/ || line ~ /^[[:space:]]*$/) next
      if(match(line, "^[[:space:]]*" key ":[[:space:]]*")){
        sub("^[[:space:]]*" key ":[[:space:]]*", "", line)
        print unquote(line)
        exit 0
      }
    }
  ' "$file"
}

yaml_list() {
  local file="$1"
  local key="$2"
  awk -v key="$key" '
    function trim(s){ sub(/^[ \t]+/,"",s); sub(/[ \t]+$/,"",s); return s }
    function unquote(s){
      s=trim(s)
      gsub(/^"/,"",s); gsub(/"$/,"",s)
      gsub(/^'\''/,"",s); gsub(/'\''$/,"",s)
      return s
    }
    BEGIN{inside=0}
    {
      line=$0
      sub(/\r$/,"",line)
      if(line ~ /^[[:space:]]*#/ || line ~ /^[[:space:]]*$/) next

      if(match(line, "^[[:space:]]*" key ":[[:space:]]*$")){
        inside=1
        next
      }

      if(inside && match(line, "^[^[:space:]]")){
        inside=0
      }

      if(inside && match(line, "^[[:space:]]*-[[:space:]]+")){
        sub("^[[:space:]]*-[[:space:]]+", "", line)
        line=unquote(line)
        if(line!="") print line
      }
    }
  ' "$file"
}
