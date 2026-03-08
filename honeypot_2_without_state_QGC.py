# this code generates heartbeat from llm at the beginning then keeps sending heartbeat
# going to experiment with telemetry and commands

# comment 8 March : we will fix the heartbeat update issue first.
from dataclasses import dataclass, field
from typing import Tuple, List
import socket
import time
import threading
from pymavlink import mavutil

from dataclasses import dataclass
from typing import Dict
import threading

from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Any, Optional, List
import threading
import requests
import json, os, re, math
from collections import defaultdict, deque

# ----------- RAG Integration -----
RAG_FILES = [
    "../RAGS/mavlink_rag_corpus.jsonl",
    "../out3/px4_command_sequences.jsonl",
    "../out3/px4_command_trace.jsonl",
]

# ============================================================
# STATE
# ============================================================


'''

| Message             | PX4         | ArduPilot |
| ------------------- | ----------- | --------- |
| HEARTBEAT           | ✅ Always    | ✅ Always  |
| SYS_STATUS          | ✅ Yes       | ✅ Yes     |
| GPS_RAW_INT         | ✅ Yes       | ✅ Yes     |
| GLOBAL_POSITION_INT | ✅ Yes       | ✅ Yes     |
| LOCAL_POSITION_NED  | ✅ Yes       | ✅ Yes     |
| ATTITUDE            | ✅ Yes       | ✅ Yes     |
| VFR_HUD             | ⚠ Sometimes | ✅ Often   |



| Message                    | PX4                | ArduPilot        |
| -------------------------- | ------------------ | ---------------- |
| ATTITUDE_TARGET            | ❌ Only in OFFBOARD | ❌ Only in GUIDED |
| POSITION_TARGET_LOCAL_NED  | ❌ OFFBOARD only    | ❌ GUIDED only    |
| POSITION_TARGET_GLOBAL_INT | ❌ Rare             | ❌ Rare           |
| ATTITUDE_QUATERNION        | ⚠ Optional         | ⚠ Optional       |



| Message          | PX4           | ArduPilot                              |
| ---------------- | ------------- | -------------------------------------- |
| SERVO_OUTPUT_RAW | ❌ Not typical | ✅ Very common (especially Plane/Rover) |
| MISSION_CURRENT  | ✅ Yes         | ✅ Yes                                  |


'''

@dataclass
class StreamConfig:
    rate_hz: float
    next_send: float



@dataclass
class CommonState:
    # ------------------------
    # Time bases
    # ------------------------
    time_boot_ms: int = 0     # used by many messages
    time_usec: int = 0        # used by GPS_RAW_INT, SERVO_OUTPUT_RAW, etc.

    # ------------------------
    # HEARTBEAT
    # ------------------------
    hb_type: int = 2          # MAV_TYPE_QUADROTOR (example)
    hb_autopilot: int = 12    # MAV_AUTOPILOT_PX4 (example)
    base_mode: int = 0
    custom_mode: int = 0
    system_status: int = 4
    mavlink_version: int = 3

    # ------------------------
    # SYS_STATUS
    # ------------------------
    voltage_battery: int = 12000          # mV
    current_battery: int = -1             # cA (10*mA). -1 = not measured
    battery_remaining: int = 90           # %
    load: int = 400                       # 0..1000
    onboard_control_sensors_present: int = 0
    onboard_control_sensors_enabled: int = 0
    onboard_control_sensors_health: int = 0
    drop_rate_comm: int = 0
    errors_comm: int = 0
    errors_count1: int = 0
    errors_count2: int = 0
    errors_count3: int = 0
    errors_count4: int = 0

    # ------------------------
    # GPS_RAW_INT
    # ------------------------
    gps_time_usec: int = 0                 # can mirror time_usec
    gps_lat: int = 0                       # 1e7 deg
    gps_lon: int = 0                       # 1e7 deg
    gps_alt: int = 0                       # mm (MSL)
    gps_alt_ellipsoid: int = 0             # mm
    gps_fix_type: int = 3
    gps_eph: int = 100                     # cm
    gps_epv: int = 100                     # cm
    gps_vel: int = 0                       # cm/s
    gps_cog: int = 0                       # cdeg
    gps_satellites_visible: int = 10
    gps_h_acc: int = 0                     # mm
    gps_v_acc: int = 0                     # mm
    gps_vel_acc: int = 0                   # mm/s
    gps_hdg_acc: int = 0                   # cdeg
    gps_yaw: int = 0                       # cdeg (if used)

    # ------------------------
    # GLOBAL_POSITION_INT
    # ------------------------
    gpi_time_boot_ms: int = 0
    gpi_lat: int = 0
    gpi_lon: int = 0
    gpi_alt: int = 0                       # mm
    gpi_relative_alt: int = 0              # mm
    gpi_vx: int = 0                        # cm/s
    gpi_vy: int = 0                        # cm/s
    gpi_vz: int = 0                        # cm/s
    gpi_hdg: int = 0                       # cdeg

    # ------------------------
    # LOCAL_POSITION_NED //SKIP for now
    # ------------------------
    lpn_time_boot_ms: int = 0
    lpn_x: float = 0.0                     # meters
    lpn_y: float = 0.0
    lpn_z: float = 0.0                     # NED: down is +, up is -
    lpn_vx: float = 0.0                    # m/s
    lpn_vy: float = 0.0
    lpn_vz: float = 0.0

    # ------------------------
    # ATTITUDE
    # ------------------------
    att_time_boot_ms: int = 0
    roll: float = 0.0                      # rad
    pitch: float = 0.0
    yaw: float = 0.0
    rollspeed: float = 0.0                 # rad/s
    pitchspeed: float = 0.0
    yawspeed: float = 0.0

    # ------------------------
    # VFR_HUD
    # ------------------------
    vfr_airspeed: float = 0.0              # m/s
    vfr_groundspeed: float = 0.0           # m/s
    vfr_heading: int = 0                   # deg
    vfr_throttle: int = 0                  # %
    vfr_alt: float = 0.0                   # m
    vfr_climb: float = 0.0                 # m/s

    # ------------------------
    # ATTITUDE_QUATERNION || Skip for now
    # ------------------------
    aq_time_boot_ms: int = 0
    q1: float = 1.0
    q2: float = 0.0
    q3: float = 0.0
    q4: float = 0.0
    repr_offset_q: Tuple[float, float, float, float] = (0.0, 0.0, 0.0, 0.0)

    # ------------------------
    # ATTITUDE_TARGET || Skip for now
    # ------------------------
    at_time_boot_ms: int = 0
    at_type_mask: int = 0
    at_q: Tuple[float, float, float, float] = (1.0, 0.0, 0.0, 0.0)
    body_roll_rate: float = 0.0
    body_pitch_rate: float = 0.0
    body_yaw_rate: float = 0.0
    thrust: float = 0.0

    # ------------------------
    # POSITION_TARGET_LOCAL_NED
    # ------------------------
    ptl_time_boot_ms: int = 0
    ptl_coordinate_frame: int = 1
    ptl_type_mask: int = 0
    ptl_x: float = 0.0
    ptl_y: float = 0.0
    ptl_z: float = 0.0
    ptl_vx: float = 0.0
    ptl_vy: float = 0.0
    ptl_vz: float = 0.0
    ptl_afx: float = 0.0
    ptl_afy: float = 0.0
    ptl_afz: float = 0.0
    ptl_yaw: float = 0.0
    ptl_yaw_rate: float = 0.0

    # ------------------------
    # POSITION_TARGET_GLOBAL_INT
    # ------------------------
    ptg_time_boot_ms: int = 0
    ptg_coordinate_frame: int = 6
    ptg_type_mask: int = 0
    ptg_lat_int: int = 0
    ptg_lon_int: int = 0
    ptg_alt: float = 0.0
    ptg_vx: float = 0.0
    ptg_vy: float = 0.0
    ptg_vz: float = 0.0
    ptg_afx: float = 0.0
    ptg_afy: float = 0.0
    ptg_afz: float = 0.0
    ptg_yaw: float = 0.0
    ptg_yaw_rate: float = 0.0

    # ------------------------
    # MISSION_CURRENT
    # ------------------------
    mission_seq: int = 0
    mission_total: int = 0
    mission_state: int = 0
    mission_mode: int = 0

    # ------------------------
    # SERVO_OUTPUT_RAW
    # ------------------------
    servo_time_usec: int = 0
    servo_port: int = 0
    servo_raw: Tuple[int, ...] = (1500,) * 16  # servo1..servo16


# /////////// commands /////////////

# ============================
# HISTORY BUFFER (last 10 HB + last 10 TELEM + last 10 CMDS)
# ============================
class HistoryBuffer:
    def __init__(self, max_hb=10, max_telem=10, max_cmd=10):
        self.hb = deque(maxlen=max_hb)         # [{"ts":..., "base_mode":..., ...}]
        self.telem = deque(maxlen=max_telem)   # [{"ts":..., "name":..., "fields":{...}}]
        self.cmd = deque(maxlen=max_cmd)       # [{"ts":..., "command":..., "params":{...}}]

    def add_hb(self, state_obj) -> None:
        self.hb.append({
            "ts": time.time(),
            "hb_type": int(state_obj.hb_type),
            "hb_autopilot": int(state_obj.hb_autopilot),
            "base_mode": int(state_obj.base_mode),
            "custom_mode": int(state_obj.custom_mode),
            "system_status": int(state_obj.system_status),
            "mavlink_version": int(state_obj.mavlink_version),
        })

    def add_telem(self, name: str, fields: Dict[str, Any]) -> None:
        self.telem.append({
            "ts": time.time(),
            "name": str(name),
            "fields": dict(fields),
        })

    def add_cmd(self, command_id: int, params: Dict[str, Any]) -> None:
        self.cmd.append({
            "ts": time.time(),
            "command": int(command_id),
            "params": dict(params),
        })

    def snapshot(self) -> Dict[str, Any]:
        return {
            "last_heartbeat": list(self.hb),
            "last_telemetry": list(self.telem),
            "recent_commands": list(self.cmd),
        }


# ============================
# RAG LOADER (command transitions)
# ============================
def load_command_rag_jsonl(path: str) -> List[Dict[str, Any]]:
    rows = []
    try:
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except Exception as e:
        print(f"[RAG] failed to load {path}: {e}", flush=True)
    print(f"[RAG] loaded {len(rows)} examples from {path}", flush=True)
    return rows


# def rag_retrieve_examples(rows: List[Dict[str, Any]], command_id: int, k: int = 3) -> List[Dict[str, Any]]:
#     out = []
#     for ex in rows:
#         cmd = ex.get("command", {})
#         ex_id = cmd.get("id") or cmd.get("command") or ex.get("command_id") or ex.get("cmd_id")
#         try:
#             if int(ex_id) == int(command_id):
#                 out.append(ex)
#         except Exception:
#             pass
#         if len(out) >= k:
#             break
#     return out

def rag_retrieve_examples(rows: List[Dict[str, Any]], command_id: int, k: int = 3) -> List[Dict[str, Any]]:
    out = []
    for ex in rows:
        if not isinstance(ex, dict):
            continue

        cmd = ex.get("command", {})

        # cmd can be dict OR int depending on jsonl format
        if isinstance(cmd, dict):
            ex_id = cmd.get("id") or cmd.get("command") or ex.get("command_id") or ex.get("cmd_id")
        else:
            # cmd is int/str
            ex_id = cmd

        try:
            if int(ex_id) == int(command_id):
                out.append(ex)
        except Exception:
            pass

        if len(out) >= k:
            break
    return out


# ============================
# ACK mapping (LLM string -> MAV_RESULT)
# ============================
ACK_RESULT_MAP = {
    "ACCEPTED": mavutil.mavlink.MAV_RESULT_ACCEPTED,
    "DENIED": mavutil.mavlink.MAV_RESULT_DENIED,
    "UNSUPPORTED": mavutil.mavlink.MAV_RESULT_UNSUPPORTED,
    "TEMPORARILY_REJECTED": mavutil.mavlink.MAV_RESULT_TEMPORARILY_REJECTED,
}
#  ////// commands ends ///////




# ============================================================
# TIME UPDATE FUNCTION (from your code)
# ============================================================

def update_time_fields(state: CommonState, boot_time: float):
    dt = max(0.0, time.monotonic() - boot_time)
    state.time_boot_ms = int(dt * 1000)
    state.time_usec = int(dt * 1_000_000.0)   # <-- ADDing this for telemetry GPS

# ============================================================
# HEARTBEAT BUILDER (from your code)
# ============================================================

def make_heartbeat(mav, state: CommonState):
    return mav.heartbeat_encode(
        int(state.hb_type),
        int(state.hb_autopilot),
        int(state.base_mode),
        int(state.custom_mode),
        int(state.system_status),
    )

def extract_json(text: str):
    """
    Extract first JSON object from LLM response.
    Handles cases where model adds extra text.
    """
    try:
        start = text.index("{")
        end = text.rindex("}") + 1
        return json.loads(text[start:end])
    except Exception as e:
        print(f"[JSON EXTRACT ERROR] {e}")
        return None


# ////////// telemetry //////////
def make_sys_status(mav, s: CommonState):
    return mav.sys_status_encode(
        int(s.onboard_control_sensors_present),
        int(s.onboard_control_sensors_enabled),
        int(s.onboard_control_sensors_health),
        int(s.load),
        int(s.voltage_battery),
        int(s.current_battery),
        int(s.battery_remaining),
        int(s.drop_rate_comm),
        int(s.errors_comm),
        int(s.errors_count1),
        int(s.errors_count2),
        int(s.errors_count3),
        int(s.errors_count4),
    )

def make_gps_raw_int(mav, s: CommonState):
    return mav.gps_raw_int_encode(
        int(s.time_usec),
        int(s.gps_fix_type),
        int(s.gps_lat),
        int(s.gps_lon),
        int(s.gps_alt),
        int(s.gps_eph),
        int(s.gps_epv),
        int(s.gps_vel),
        int(s.gps_cog),
        int(s.gps_satellites_visible),
    )

def make_global_position_int(mav, s: CommonState):
    return mav.global_position_int_encode(
        int(s.time_boot_ms),
        int(s.gpi_lat),
        int(s.gpi_lon),
        int(s.gpi_alt),
        int(s.gpi_relative_alt),
        int(s.gpi_vx),
        int(s.gpi_vy),
        int(s.gpi_vz),
        int(s.gpi_hdg),
    )

def make_attitude(mav, s: CommonState):
    return mav.attitude_encode(
        int(s.time_boot_ms),
        float(s.roll),
        float(s.pitch),
        float(s.yaw),
        float(s.rollspeed),
        float(s.pitchspeed),
        float(s.yawspeed),
    )

def make_vfr_hud(mav, s: CommonState):
    return mav.vfr_hud_encode(
        float(s.vfr_airspeed),
        float(s.vfr_groundspeed),
        int(s.vfr_heading),
        int(s.vfr_throttle),
        float(s.vfr_alt),
        float(s.vfr_climb),
    )

TELEM_BUILDERS = {
    "SYS_STATUS": make_sys_status,
    "GPS_RAW_INT": make_gps_raw_int,
    "GLOBAL_POSITION_INT": make_global_position_int,
    "ATTITUDE": make_attitude,
    "VFR_HUD": make_vfr_hud
    # HEARTBEAT stays in heartbeat_loop()
}
# ////////// telemetry //////////



# ////////////// for commands /////////////
def load_rag_transitions(self, path: str) -> None:
    self.rag_transitions = []
    with open(path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                self.rag_transitions.append(json.loads(line))
            except Exception:
                continue

def rag_retrieve(self, command_id: int, k: int = 3) -> list:
    # simplest: filter by command id/name field that exists in your jsonl
    out = []
    for ex in getattr(self, "rag_transitions", []):
        cmd = ex.get("command", {})
        # adjust this based on your dataset field names:
        ex_id = cmd.get("id") or cmd.get("command") or cmd.get("name")
        if ex_id == command_id:
            out.append(ex)
        if len(out) >= k:
            break
    return out

#  /////////// commands //////////

#  /////////// command_ack //////////
# ----------------------------------------
# Rule based ACK logic
# ----------------------------------------
def _is_armed(state) -> bool:
    # PX4: armed flag is base_mode bit 7 (0x80)
    return (int(getattr(state, "base_mode", 0)) & 0x80) != 0

def _set_armed_patch(state, armed: bool):
    bm = int(getattr(state, "base_mode", 0))
    if armed:
        bm |= 0x80
        return {"base_mode": bm, "system_status": 4}   # ACTIVE
    else:
        bm &= ~0x80
        return {"base_mode": bm, "system_status": 3}   # STANDBY

def _alt_m(state) -> float:
    # Prefer relative altitude in mm if you have it; else use vfr_alt; else 0
    if hasattr(state, "gpi_relative_alt"):
        return float(getattr(state, "gpi_relative_alt", 0)) / 1000.0
    if hasattr(state, "vfr_alt"):
        return float(getattr(state, "vfr_alt", 0.0))
    if hasattr(state, "gps_alt"):
        return float(getattr(state, "gps_alt", 0)) / 1000.0
    return 0.0

def rule_based_ack(cmd, params, state):
    alt = _alt_m(state)
    armed = _is_armed(state)

    # ARM/DISARM (400)
    if cmd == mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM:
        arm_flag = int(round(float(params.get("param1", 0.0))))

        if arm_flag == 1:  # ARM
            if not armed:
                return mavutil.mavlink.MAV_RESULT_ACCEPTED, "arm allowed"
            else:
                return mavutil.mavlink.MAV_RESULT_TEMPORARILY_REJECTED, "already armed"

        else:  # DISARM
            if alt < 0.2:
                return mavutil.mavlink.MAV_RESULT_ACCEPTED, "disarm allowed"
            else:
                return mavutil.mavlink.MAV_RESULT_DENIED, "cannot disarm in air"

    # TAKEOFF (22)
    if cmd == mavutil.mavlink.MAV_CMD_NAV_TAKEOFF:
        target_alt = float(params.get("param7", 3.0))

        if armed and alt < 0.3 and target_alt > 0.5:
            return mavutil.mavlink.MAV_RESULT_ACCEPTED, "takeoff allowed"
        else:
            return mavutil.mavlink.MAV_RESULT_DENIED, "takeoff preconditions failed"

    # LAND (21)
    if cmd == mavutil.mavlink.MAV_CMD_NAV_LAND:
        if armed and alt > 0.5:
            return mavutil.mavlink.MAV_RESULT_ACCEPTED, "landing allowed"
        else:
            return mavutil.mavlink.MAV_RESULT_TEMPORARILY_REJECTED, "not flying"

    # WAYPOINT (16)
    if cmd == mavutil.mavlink.MAV_CMD_NAV_WAYPOINT:
        if armed:
            return mavutil.mavlink.MAV_RESULT_ACCEPTED, "waypoint accepted"
        else:
            return mavutil.mavlink.MAV_RESULT_DENIED, "not armed"

    return mavutil.mavlink.MAV_RESULT_UNSUPPORTED, "unsupported command"
#  /////////// command_ack //////////


# ============================================================
# CONNECTIVITY CLASS (extracted from LLMHoneypot)
# ============================================================

class LLMHoneypot:

    # ---------------------------
    # __init__  (GCS socket setup)
    # ---------------------------
    def __init__(self, listen_ip="0.0.0.0", listen_port=14550):

        self.listen_ip = listen_ip
        self.listen_port = listen_port

        # UDP socket setup  ← from your code
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        #self.sock.bind((self.listen_ip, self.listen_port)) //for qgc
        self.sock.bind(("127.0.0.1", 14551)) # for qgc new
        self.gcs_addr = ("127.0.0.1", 14550) #for qgc new
        self.sock.settimeout(0.2)

        # MAVLink encoder/decoder
        self.mav_out = mavutil.mavlink.MAVLink(None)
        self.mav_out.srcSystem = 1
        self.mav_out.srcComponent = 1

        self.mav_in = mavutil.mavlink.MAVLink(None)

        #self.gcs_addr = None  //for qgc
        self.state = CommonState()
        self.boot_time = time.monotonic()

        print(f"[INIT] Listening on {self.listen_ip}:{self.listen_port}")

        self.llm_enabled = True
        self.llm_log = []
        self.state_lock = threading.Lock()
        self.identity_locked = False


        # /////// telemetry helpers ///////
        self.stream_lock = threading.Lock()
        self.streams = {}  # name -> [rate_hz, next_send]
        # heartbeat is handled by heartbeat_loop, so don't add it here

        self.telemetry_stop = threading.Event()
        self.telemetry_thread = threading.Thread(target=self.telemetry_loop, daemon=True)

        # //////// command helpers ///////
        # --- history buffers ---
        self.hist = HistoryBuffer(max_hb=10, max_telem=10, max_cmd=10)

        # --- command rag (mounted file path from your upload) ---
        # self.command_rag_path = "../outx/rag_command_transitions.jsonl"
        # # print("oooooo" + str(os.path.exists(self.command_rag_path))) ## delete later
        # self.command_rag_rows = load_command_rag_jsonl(self.command_rag_path)
        # ---  command RAG sources (few-shot for LLM command reasoning) ---
        self.cmd_trace_rows = load_command_rag_jsonl("../out/px4_command_trace.jsonl")
        self.cmd_seq_rows   = load_command_rag_jsonl("../out/px4_command_sequences.jsonl")

        # --- telemetry override queue (10-step series after a command) ---
        self.override_lock = threading.Lock()
        self.override_series = deque()  # each item: {"apply_at": float_monotonic, "fields": {...}}


        # ////////// for QGC
        self.params = [
            ("SYS_AUTOSTART", 4010.0, mavutil.mavlink.MAV_PARAM_TYPE_INT32),
            ("COM_ARM_WO_GPS", 0.0,   mavutil.mavlink.MAV_PARAM_TYPE_INT32),
            ("MPC_XY_VEL_MAX", 5.0,   mavutil.mavlink.MAV_PARAM_TYPE_REAL32),
            ("MIS_TAKEOFF_ALT", 10.0, mavutil.mavlink.MAV_PARAM_TYPE_REAL32),
            ("RTL_RETURN_ALT", 15.0,  mavutil.mavlink.MAV_PARAM_TYPE_REAL32),
        ]
        self.param_index = {name: i for i, (name, _, _) in enumerate(self.params)}
        # ////////// for QGC


    # //////// command helpers ///////

    def log_llm_io(self, tag: str, system_text: str, user_text: str, raw: str, parsed: dict, latency_ms: float):
        row = {
            "ts": time.time(),
            "tag": tag,                    # e.g., "startup_identity", "command"
            "latency_ms": latency_ms,
            "system_text": system_text,    # optional (can be huge)
            "user_text": user_text,        # optional (can be huge)
            "raw": raw,
            "parsed": parsed,
        }
        try:
            with open("llm_io_log.jsonl", "a") as f:
                f.write(json.dumps(row) + "\n")
        except Exception as e:
            print(f"[LLM LOG ERROR] {e}", flush=True)

    # ---------------------------
    # send_mav()  (GCS TX)
    # ---------------------------
    def send_mav(self, msg):
        if not self.gcs_addr:
            return
        pkt = msg.pack(self.mav_out)
        self.sock.sendto(pkt, self.gcs_addr)
        print("[TX UDP dst]", self.gcs_addr, "bytes=", len(pkt), flush=True) #debug delete later dlt



    # ---------------------------
    # handle_inbound_msg()
    # ---------------------------
    # def handle_inbound_msg(self, msg):

    #     msg_type = msg.get_type()

    #     if msg_type == "COMMAND_LONG":
    #         cmd = msg.command

    #         if cmd == mavutil.mavlink.MAV_CMD_COMPONENT_ARM_DISARM:
    #             arm = int(msg.param1)

    #             if arm == 1:
    #                 self.state.base_mode |= 0x80
    #                 print("[ARMED]")
    #             else:
    #                 self.state.base_mode &= ~0x80
    #                 print("[DISARMED]")

    #             ack = self.mav_out.command_ack_encode(
    #                 cmd,
    #                 mavutil.mavlink.MAV_RESULT_ACCEPTED
    #             )
    #             self.send_mav(ack)


    # ---------------------------
    # heartbeat_loop()
    # ---------------------------
    # def heartbeat_loop(self):
    #     while True:
    #         if self.gcs_addr:
    #             update_time_fields(self.state, self.boot_time)
    #             hb = make_heartbeat(self.mav_out, self.state)
    #             self.send_mav(hb)
    #         time.sleep(1)

    # ///////////////HEARTBEAT /////////////////////////

    def heartbeat_loop(self):
        while True:
            if self.gcs_addr:
                update_time_fields(self.state, self.boot_time)               
    #  to make llm give heartbeat all the time | uncomment below ////////////////////////
                # response = self.llm_prompt({"event": "heartbeat_tick"},context_type="heartbeat")
                # if response and self.verify_response(response):
                #     self.llm_output_process(response)
    #  to make llm give heartbeat all the time ////////////////////////
                # log last heartbeat snapshot (for LLM prompt)               
                hb = make_heartbeat(self.mav_out, self.state)
                print(
                    "[HEARTBEAT TX]", "base_mode=", int(self.state.base_mode),
                    "custom_mode=", int(self.state.custom_mode), "system_status=", int(self.state.system_status),
                    "armed=", bool(self.state.base_mode & 0x80),flush=True) # debug delete later dlt

                self.send_mav(hb)
                with self.state_lock: # log last 10 hb
                    self.hist.add_hb(self.state) # log last hb (for LLM prompt)
            time.sleep(1)


    def initialize_heartbeat_identity(self):
        """
        Call LLM once to initialize heartbeat identity.
        """

        response = self.llm_prompt({"event": "startup_identity"})

        if response and self.verify_response(response):

            with self.state_lock:
                for k, v in response["state_patch"].items():
                    setattr(self.state, k, v)

            print("[HEARTBEAT] Identity initialized by LLM")
            # print("[DEBUG] Initial HB State:",
            #     self.state.hb_type,
            #     self.state.hb_autopilot,
            #     self.state.base_mode,
            #     self.state.system_status)
            # self.identity_locked = True


    # ////////////////////// telemetry ///////////////////////

    def _msg_name_from_id(self, msg_id: int) -> str:
        """
        Best-effort msg_id -> message name (e.g., 1 -> SYS_STATUS).
        Works across pymavlink variants.
        """
        try:
            cls = mavutil.mavlink.mavlink_map.get(int(msg_id))
            if cls is None:
                return ""
            # Many builds provide .msgname
            name = getattr(cls, "msgname", "")
            if name:
                return str(name)
            # Fallback: class name like MAVLink_sys_status_message
            cname = getattr(cls, "__name__", "")
            if cname.startswith("MAVLink_") and cname.endswith("_message"):
                return cname[len("MAVLink_"):-len("_message")].upper()
            return cname.upper()
        except Exception:
            return ""


    def _apply_message_interval_request(self, msg_id: int, interval_us: int) -> bool:
        """
        Returns True if handled, False if unsupported/unknown.
        """
        # Normalize (COMMAND_LONG params often come in as floats)
        try:
            msg_id_i = int(round(float(msg_id)))
            interval_us_i = int(round(float(interval_us)))
        except Exception as e:
            print(f"[STREAM] invalid params msg_id={msg_id!r} interval_us={interval_us!r} err={e}", flush=True)
            return False

        name = self._msg_name_from_id(msg_id_i)
        if not name:
            print(f"[STREAM] unknown msg_id={msg_id_i} interval_us={interval_us_i}", flush=True)
            return False

        # HEARTBEAT handled elsewhere
        if name == "HEARTBEAT":
            print(f"[STREAM] ignoring HEARTBEAT request interval_us={interval_us_i}", flush=True)
            return True

        # Only stream what we can build
        if name not in TELEM_BUILDERS:
            print(f"[STREAM] not supported {name} (msg_id={msg_id_i})", flush=True)
            return False

        # disable stream
        if interval_us_i <= 0:
            with self.stream_lock:
                self.streams.pop(name, None)
            print(f"[STREAM] disabled {name}", flush=True)
            return True

        rate_hz = 1e6 / float(interval_us_i)
        rate_hz = max(0.1, min(rate_hz, 50.0))

        now = time.monotonic()
        with self.stream_lock:
            self.streams[name] = [rate_hz, now]

        print(f"[STREAM] enabled {name} (msg_id={msg_id_i}) @ {rate_hz:.2f} Hz", flush=True)
        return True



    def telemetry_loop(self):
        """
        Sends telemetry ONLY for streams requested via SET_MESSAGE_INTERVAL.
        HEARTBEAT is not handled here.
        """
        tick = 0.01  # internal scheduler rate (100 Hz)

        while not self.telemetry_stop.is_set():
            if not self.gcs_addr:
                time.sleep(tick)
                continue

            now = time.monotonic()

            with self.state_lock:
                update_time_fields(self.state, self.boot_time)



            # ///////////// Apply queued telemetry “command impact” inside
            # --- apply scheduled command-impact telemetry steps (if any) ---
            with self.override_lock:
                while self.override_series and now >= self.override_series[0]["apply_at"]:
                    step = self.override_series.popleft()
                    fields = step.get("fields", {})
                    with self.state_lock:
                        for k, v in fields.items():
                            if hasattr(self.state, k):
                                setattr(self.state, k, v)

            # ///////////// Apply queued telemetry “command impact” inside

            with self.stream_lock:
                items = list(self.streams.items())  # copy

            for name, (rate_hz, next_send) in items:
                if now < next_send:
                    continue

                builder = TELEM_BUILDERS.get(name)
                if not builder:
                    continue

                try:
                    with self.state_lock:
                        msg = builder(self.mav_out, self.state)
                    self.send_mav(msg)
                except Exception as e:
                    print(f"[TELEM ERROR] {name}: {e}", flush=True)

                    # after you successfully send a telemetry message, log what you sent (from state fields relevant to that message):
                    # log telemetry snapshot (minimal fields per message type)
                    with self.state_lock:
                        if name == "SYS_STATUS":
                            self.hist.add_telem(name, {
                                "voltage_battery": self.state.voltage_battery,
                                "current_battery": self.state.current_battery,
                                "battery_remaining": self.state.battery_remaining,
                                "load": self.state.load,
                            })
                        elif name == "GPS_RAW_INT":
                            self.hist.add_telem(name, {
                                "time_usec": self.state.time_usec,
                                "gps_fix_type": self.state.gps_fix_type,
                                "gps_lat": self.state.gps_lat,
                                "gps_lon": self.state.gps_lon,
                                "gps_alt": self.state.gps_alt,
                                "gps_vel": self.state.gps_vel,
                                "gps_cog": self.state.gps_cog,
                                "gps_satellites_visible": self.state.gps_satellites_visible,
                            })
                        elif name == "GLOBAL_POSITION_INT":
                            self.hist.add_telem(name, {
                                "time_boot_ms": self.state.time_boot_ms,
                                "gpi_lat": self.state.gpi_lat,
                                "gpi_lon": self.state.gpi_lon,
                                "gpi_alt": self.state.gpi_alt,
                                "gpi_relative_alt": self.state.gpi_relative_alt,
                                "gpi_vx": self.state.gpi_vx,
                                "gpi_vy": self.state.gpi_vy,
                                "gpi_vz": self.state.gpi_vz,
                                "gpi_hdg": self.state.gpi_hdg,
                            })
                        elif name == "ATTITUDE":
                            self.hist.add_telem(name, {
                                "time_boot_ms": self.state.time_boot_ms,
                                "roll": self.state.roll,
                                "pitch": self.state.pitch,
                                "yaw": self.state.yaw,
                                "rollspeed": self.state.rollspeed,
                                "pitchspeed": self.state.pitchspeed,
                                "yawspeed": self.state.yawspeed,
                            })
                        elif name == "VFR_HUD":
                            self.hist.add_telem(name, {
                                "vfr_airspeed": self.state.vfr_airspeed,
                                "vfr_groundspeed": self.state.vfr_groundspeed,
                                "vfr_heading": self.state.vfr_heading,
                                "vfr_throttle": self.state.vfr_throttle,
                                "vfr_alt": self.state.vfr_alt,
                                "vfr_climb": self.state.vfr_climb,
                            })
                # /////////////// telemetry logging /////

                period = 1.0 / max(0.0001, float(rate_hz))
                with self.stream_lock:
                    # stream may have been removed meanwhile
                    if name in self.streams:
                        self.streams[name][1] = now + period

            time.sleep(tick)

            # ////////////////////// telemetry ///////////////////////



    # //////////////////////QGC ///////////////////////
    def _send_param_value(self, name: str, value: float, ptype: int, index: int, count: int):
        name16 = name[:16]  # MAVLink param_id is 16 chars
        msg = self.mav_out.param_value_encode(
            name16.encode("ascii"),
            float(value),
            int(ptype),
            int(count),
            int(index),
        )
        self.send_mav(msg)

    def _send_all_params(self):
        count = len(self.params)
        for i, (name, val, ptype) in enumerate(self.params):
            self._send_param_value(name, val, ptype, i, count)
    # //////////////////////QGC ///////////////////////


    # ---------------------------
    # core()  (Main loop)
    # ---------------------------

    OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://192.168.1.22:11434")
    OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1")
    OLLAMA_TIMEOUT_SEC = 20

    LLM_OUTPUT_RULES = """
    Return ONLY valid JSON in this exact shape:
    {
    "verdict": {"label": "benign|suspicious|attack", "reason": "short"},
    "state_patch": { "<CommonState field>": <value>, ... },
    "telemetry_series": [
        {"dt": 0.0, "fields": {"<CommonState field>": <value>, ...}},
        {"dt": 0.1, "fields": {...}},
        ...
    ]
    }

    Rules:
    - telemetry_series covers ~1-3 seconds, dt is non-decreasing.
    - Only use fields that exist in CommonState.
    - Keep values plausible.
    Units (MUST follow):
    - gps_lat, gps_lon, gpi_lat, gpi_lon: integer in range -900000000 to 900000000
    - gps_alt, gpi_alt, gpi_relative_alt: int millimeters
    - gps_vel: int cm/s
    - gps_cog, gpi_hdg: int centi-degrees (0..35999)
    - roll, pitch, yaw: radians
    """

    # def call_ollama(system_text: str, user_text: str) -> str:
    # def call_ollama(self, system_text: str, user_text: str) -> str:
    def call_ollama(self, system_text: str, user_text: str, tag: str = "general") -> str:
        payload = {
            "model": self.OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": system_text},
                {"role": "user", "content": user_text}
            ],
            "stream": False,
            "format": "json",  #  FORCE VALID JSON
            "options": {
                "temperature": 0.2
                # "top_p": 1
            }
        }
        t0 = time.monotonic()
        r = requests.post(
            f"{self.OLLAMA_URL}/api/chat",
            json=payload,
            timeout=self.OLLAMA_TIMEOUT_SEC,
        )
        r.raise_for_status()
        
        raw = r.json()["message"]["content"]
        dt_ms = (time.monotonic() - t0) * 1000.0

        parsed = extract_json(raw)  # may be None
        self.log_llm_io(tag, system_text, user_text, raw, parsed, dt_ms)

        return raw


    # def llm_prompt(self, attacker_msg: Optional[dict] = None) -> Optional[dict]:
    def llm_prompt(self, attacker_msg=None, context_type="general"):
        """
        Build prompt + call Ollama + extract JSON.
        Returns parsed dict or None.
        """

        HEARTBEAT_PROMPT_PATCH = """
        Context: You are generating HEARTBEAT state updates.

        Rules:
        - Only modify: base_mode, custom_mode, system_status.
        - Do NOT modify: hb_type, hb_autopilot, mavlink_version.
        - Maintain identity consistency.
        - Armed flag is base_mode bit 7.
        - system_status must be realistic (e.g., STANDBY=3, ACTIVE=4).
        - Do not generate large sudden changes.
        """


        # -------------------------
        # 1️⃣ Snapshot state safely
        # -------------------------
        with self.state_lock:
            snapshot = self.state.__dict__.copy()

# /////////////////// HEARTBEAT /////////////////////////
        if context_type == "heartbeat":
            system_text = self.LLM_OUTPUT_RULES + "\n" + HEARTBEAT_PROMPT_PATCH
        else:
            system_text = self.LLM_OUTPUT_RULES
# /////////////////// HEARTBEAT /////////////////////////
        

        user_text = json.dumps({
            "current_state": snapshot,
            "attacker_input": attacker_msg,
            "instruction": "Evaluate situation and respond per output rules."
        })

        try:
            # -------------------------
            # 2️⃣ Call model
            # -------------------------
            # raw = call_ollama(LLM_OUTPUT_RULES, user_text)
            # print("\n[LLM PROMPT] system_text:\n", system_text)
            # print("\n[LLM PROMPT] user_text:\n", user_text)
            # raw = self.call_ollama(system_text, user_text)
            raw = self.call_ollama(system_text, user_text, tag=context_type)


            # -------------------------
            # 3️⃣ Extract JSON
            # -------------------------
            # print("\n[RAW LLM OUTPUT]\n", raw, "\n")
            parsed = extract_json(raw)

            if parsed is None:
                print("[LLM] JSON extraction failed")
                return None

            return parsed

        except Exception as e:
            print(f"[LLM ERROR] {e}")
            return None




    def verify_response(self, response: dict) -> bool:

        if not isinstance(response, dict):
            return False

        required = {"verdict", "state_patch", "telemetry_series"}
        if not required.issubset(response.keys()):
            print("[VERIFY FAIL] Missing required keys")
            return False

        valid_fields = set(self.state.__dict__.keys())

        # Validate state_patch
        state_patch = response.get("state_patch", {})
        if not isinstance(state_patch, dict):
            return False

        for k in state_patch.keys():
            if k not in valid_fields:
                print(f"[VERIFY FAIL] Invalid state field: {k}")
                return False

        # Validate telemetry_series
        series = response.get("telemetry_series", [])
        if not isinstance(series, list):
            return False

        last_dt = -1
        for step in series:
            if "dt" not in step or "fields" not in step:
                print("[VERIFY FAIL] Bad telemetry structure")
                return False

            if step["dt"] < last_dt:
                print("[VERIFY FAIL] dt not non-decreasing")
                return False

            last_dt = step["dt"]

            for k in step["fields"].keys():
                if k not in valid_fields:
                    print(f"[VERIFY FAIL] Invalid telemetry field: {k}")
                    return False
# ///////////////////////HEARTBEAT /////////////////////////
        if self.identity_locked:
            protected = {"hb_type", "hb_autopilot", "mavlink_version"}
            for k in state_patch.keys():
                if k in protected:
                    print(f"[VERIFY FAIL] Attempt to modify locked identity field: {k}")
                    return False
# ///////////////////////HEARTBEAT /////////////////////////
        return True


    def llm_output_process(self, response: dict):

        if not self.verify_response(response):
            print("[LLM] Response rejected.")
            return

        with self.state_lock:
            # Apply immediate state patch
            for k, v in response["state_patch"].items():
                setattr(self.state, k, v)

        print("[LLM] State patch applied.")



    def llm_logging(self, attacker_msg: dict, response: dict):

        entry = {
            "timestamp": time.time(),
            "attacker_input": attacker_msg,
            "verdict": response.get("verdict"),
            "state_patch": response.get("state_patch"),
            "telemetry_steps": len(response.get("telemetry_series", []))
        }

        self.llm_log.append(entry)

        try:
            with open("llm_decision_log.jsonl", "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            print(f"[LOG ERROR] {e}")



# ////////////handle commands////////////////////////
    # logging the command and response
    def log_cmd_vs_llm(self, cmd: int, params: dict, llm_raw: str, llm_parsed: dict):
        row = {
            "ts": time.time(),
            "cmd": int(cmd),
            "params": params,
            "llm_raw": llm_raw,
            "llm_parsed": llm_parsed,
        }
        try:
            with open("./logs/cmd_llm_log.jsonl", "a") as f:
                f.write(json.dumps(row) + "\n")
            print("[LOG] log_cmd_vs_llm CALLED", flush=True)
            print("[LOG] path=./logs/cmd_llm_log.jsonl", flush=True)
        except Exception as e:
            print(f"[CMD_LLM_LOG_ERROR] {e}", flush=True)

    def llm_prompt_command(self, command_id: int, params: Dict[str, Any]) -> Optional[dict]:
        # Snapshot state + history
        with self.state_lock:
            snapshot = self.state.__dict__.copy()
        hist = self.hist.snapshot()

        # fewshot = rag_retrieve_examples(self.command_rag_rows, command_id, k=3)
        fewshot_trace = rag_retrieve_examples(self.cmd_trace_rows, command_id, k=2)
        fewshot_seq   = rag_retrieve_examples(self.cmd_seq_rows, command_id, k=2)

        fewshot = {
            "trace_examples": fewshot_trace,
            "sequence_examples": fewshot_seq,
        }

        system_text = """
You are an autopilot behavior generator for a MAVLink honeypot.
Return ONLY valid JSON. No prose.

Output EXACTLY:
{
  "ack": {"command": <int>, "result": "ACCEPTED|DENIED|UNSUPPORTED|TEMPORARILY_REJECTED", "reason": "short"},
  "heartbeat_patch": {"base_mode": <int optional>, "custom_mode": <int optional>, "system_status": <int optional>},
  "state_patch": { "<CommonState field>": <value>, ... },
  "telemetry_series": [
    {"dt": 0.0, "fields": {...}},
    ...
    {"dt": 0.4, "fields": {...}}
  ]
}

Rules:
- telemetry_series MUST have exactly 5 steps
- dt values MUST be: 0.0, 0.1, 0.2, 0.3, 0.4 (no other dt allowed).
- Do NOT include dt=0.5 or dt=1.0.
- Only use fields that exist in CommonState.
- Smooth realistic changes (no big jumps).
- heartbeat_patch may ONLY change base_mode, custom_mode, system_status.
- base_mode armed flag is bit 7 (0x80); system_status: 3=STANDBY, 4=ACTIVE.
""".strip()

        user_text = json.dumps({
            "command": {"id": int(command_id), "params": params},
            "current_state": snapshot,
            "history": hist,
            "fewshot": fewshot,
            "instruction": "Generate ACK and the next 1 second of telemetry impact from this command."
        })

        try:
            # raw = self.call_ollama(system_text, user_text)
            # raw = self.call_ollama(system_text, user_text, tag="command")
            # parsed = extract_json(raw)
            # return parsed
            raw = self.call_ollama(system_text, user_text, tag="command")
            parsed = extract_json(raw)
            return {"_raw": raw, "_parsed": parsed}
        except Exception as e:
            print(f"[LLM CMD ERROR] {e}", flush=True)
            return None


    def apply_llm_command_result(self, response: dict) -> None:
        """
        1) Send COMMAND_ACK
        2) Apply heartbeat_patch + state_patch immediately
        3) Schedule telemetry_series over next 1 second (10 steps)
        """
        if not isinstance(response, dict):
            return

        ack = response.get("ack", {})
        cmd_id = int(ack.get("command", -1)) if isinstance(ack, dict) else -1
        result_str = (ack.get("result") if isinstance(ack, dict) else "UNSUPPORTED") or "UNSUPPORTED"
        result_str = str(result_str).upper().strip()
        mav_result = ACK_RESULT_MAP.get(result_str, mavutil.mavlink.MAV_RESULT_UNSUPPORTED)

        # //// commented out for ack time delay /////
        # if cmd_id >= 0:
        #     ack_msg = self.mav_out.command_ack_encode(cmd_id, mav_result)
        #     self.send_mav(ack_msg)
        # //// commented out for ack time delay /////
        

        # apply patches
        hb_patch = response.get("heartbeat_patch", {}) if isinstance(response.get("heartbeat_patch", {}), dict) else {}
        state_patch = response.get("state_patch", {}) if isinstance(response.get("state_patch", {}), dict) else {}

        with self.state_lock:
            for k, v in hb_patch.items():
                if k in ("base_mode", "custom_mode", "system_status"):
                    setattr(self.state, k, v)
            for k, v in state_patch.items():
                if hasattr(self.state, k):
                    setattr(self.state, k, v)

        # schedule telemetry series
        series = response.get("telemetry_series", [])
        if not isinstance(series, list) or len(series) == 0:
            return

        base = time.monotonic()
        with self.override_lock:
            self.override_series.clear()
            for step in series:
                dt = float(step.get("dt", 0.0))
                fields = step.get("fields", {})
                if not isinstance(fields, dict):
                    continue
                self.override_series.append({
                    "apply_at": base + dt,
                    "fields": fields
                })

        print(f"[CMD APPLY] ack={result_str} scheduled_steps={len(self.override_series)}", flush=True)

# ////////////handle commands////////////////////////


#     def handle_inbound_msg(self, msg):

#         msg_type = msg.get_type()
#         attacker_msg = {"type": msg_type}

# # ////////////// telemetry /////////////////////
#         # --- Stream requests can arrive in TWO forms ---

#         # (A) Direct SET_MESSAGE_INTERVAL message
#         if msg_type == "SET_MESSAGE_INTERVAL":
#             msg_id = int(getattr(msg, "message_id", -1))
#             interval_us = int(getattr(msg, "interval_us", 0))
#             self._apply_message_interval_request(msg_id, interval_us)
#             return

#         # (B) COMMAND_LONG using MAV_CMD_SET_MESSAGE_INTERVAL
#         # -------------------------
#         if msg_type == "COMMAND_LONG":
#             cmd = int(getattr(msg, "command", -1))

#             # ignore interval command here (already handled above)
#             if cmd == mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL:
#                 return

#             params = {
#                 "param1": float(getattr(msg, "param1", 0.0)),
#                 "param2": float(getattr(msg, "param2", 0.0)),
#                 "param3": float(getattr(msg, "param3", 0.0)),
#                 "param4": float(getattr(msg, "param4", 0.0)),
#                 "param5": float(getattr(msg, "param5", 0.0)),
#                 "param6": float(getattr(msg, "param6", 0.0)),
#                 "param7": float(getattr(msg, "param7", 0.0)),
#             }

#             # log command into history
#             self.hist.add_cmd(cmd, params)

#             # ask LLM
#             resp = self.llm_prompt_command(cmd, params)
#             if resp:
#                 self.apply_llm_command_result(resp)
#             else:
#                 # fallback: unsupported ack
#                 ack_msg = self.mav_out.command_ack_encode(cmd, mavutil.mavlink.MAV_RESULT_UNSUPPORTED)
#                 self.send_mav(ack_msg)

#             return

    def handle_inbound_msg(self, msg):
        """
        # Handles inbound MAVLink messages from the GCS.

        # Priority order:
        # 1) Telemetry stream control (SET_MESSAGE_INTERVAL / MAV_CMD_SET_MESSAGE_INTERVAL)
        # - handled immediately
        # - always ACKed (ACCEPTED if supported else UNSUPPORTED)
        # - NEVER sent to the LLM

        # 2) Other COMMAND_LONG commands
        # - ACK immediately (so GCS sees a response)
        # - optionally send to LLM to generate state_patch + telemetry_series
        # - apply patch to state (which affects subsequent telemetry + heartbeat)

        update: Full patched handler:
        - Stream control (SET_MESSAGE_INTERVAL / MAV_CMD_SET_MESSAGE_INTERVAL): handled locally, ACKed, NOT sent to LLM
        - Other COMMAND_LONG: logged -> LLM (llm_prompt_command) -> apply_llm_command_result (ACK + patches + schedule series)
        - Everything else: ignore (or extend later)
            """
        msg_type = msg.get_type()
        attacker_msg = {"type": msg_type}

        # to fix the ack thing /////////
        # ts = int(getattr(msg, "target_system", 1))
        # tc = int(getattr(msg, "target_component", 1))
        # to fix the ack thing /////////

        # ts = msg.get_srcSystem()
        # tc = msg.get_srcComponent()    

        ts = int(getattr(msg, "get_srcSystem", lambda: 255)())
        tc = int(getattr(msg, "get_srcComponent", lambda: 190)())


        # ------------------------------------------------------------
        # (A) Direct SET_MESSAGE_INTERVAL message
        # ------------------------------------------------------------
        if msg_type == "SET_MESSAGE_INTERVAL":
            msg_id = int(getattr(msg, "message_id", -1))
            interval_us = int(getattr(msg, "interval_us", 0))

            handled = self._apply_message_interval_request(msg_id, interval_us)

            # Some GCS don't require ACK for SET_MESSAGE_INTERVAL,
            # but sending COMMAND_ACK for MAV_CMD_SET_MESSAGE_INTERVAL is harmless if you want.
            # We'll keep it quiet here to avoid confusing GCS tooling.
            return

        #  ////////// QGC ///////////////////////////////////////
        if msg_type == "PARAM_REQUEST_LIST":
            print("[PARAM] REQUEST_LIST -> sending params", flush=True)
            self._send_all_params()
            return

        if msg_type == "PARAM_REQUEST_READ":
            idx = int(getattr(msg, "param_index", -1))
            pid = getattr(msg, "param_id", b"")

            try:
                pname = pid.decode("ascii", errors="ignore").strip("\x00").strip()
            except Exception:
                pname = ""

            if 0 <= idx < len(self.params):
                name, val, ptype = self.params[idx]
                print(f"[PARAM] REQUEST_READ idx={idx} -> {name}", flush=True)
                self._send_param_value(name, val, ptype, idx, len(self.params))
                return

            if pname and pname in self.param_index:
                i = self.param_index[pname]
                name, val, ptype = self.params[i]
                print(f"[PARAM] REQUEST_READ name={pname} -> {name}", flush=True)
                self._send_param_value(name, val, ptype, i, len(self.params))
                return

            print(f"[PARAM] REQUEST_READ unknown idx={idx} name={pname!r}", flush=True)
            return
        #  ////////// QGC /////////////////////////////

        # ------------------------------------------------------------
        # (B) COMMAND_LONG: may include MAV_CMD_SET_MESSAGE_INTERVAL
        # ------------------------------------------------------------
        if msg_type == "COMMAND_LONG":
            cmd = int(getattr(msg, "command", -1))
            attacker_msg["command"] = cmd

            # ---------------------------
            # B1) Telemetry stream request
            # ---------------------------
            if cmd == mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL:
                # param1 = message_id, param2 = interval_us (often floats)
                msg_id = int(round(float(getattr(msg, "param1", -1.0))))
                interval_us = int(round(float(getattr(msg, "param2", 0.0))))

                handled = self._apply_message_interval_request(msg_id, interval_us)

                # ack = self.mav_out.command_ack_encode( #commented for ack handling
                #     mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL, #commented for ack handling
                #     mavutil.mavlink.MAV_RESULT_ACCEPTED if handled else mavutil.mavlink.MAV_RESULT_UNSUPPORTED #commented for ack handling
                # ) #commented for ack handling
                # self.send_mav(ack) #commented for ack handling

                ack = self.mav_out.command_ack_encode(
                    mavutil.mavlink.MAV_CMD_SET_MESSAGE_INTERVAL,
                    mavutil.mavlink.MAV_RESULT_ACCEPTED if handled else mavutil.mavlink.MAV_RESULT_UNSUPPORTED                    # ,0,  # progress
                    # 0,  # result_param2
                    # ts, # target_system
                    # tc  # target_component
                    )
                self.send_mav(ack)
                return

            # ---------------------------
            # B2) Other commands (ARM/DISARM/TAKEOFF/etc.)
            # ---------------------------

            # --- B2) All other commands: route to LLM command pipeline ---
            params = {
                "param1": float(getattr(msg, "param1", 0.0)),
                "param2": float(getattr(msg, "param2", 0.0)),
                "param3": float(getattr(msg, "param3", 0.0)),
                "param4": float(getattr(msg, "param4", 0.0)),
                "param5": float(getattr(msg, "param5", 0.0)),
                "param6": float(getattr(msg, "param6", 0.0)),
                "param7": float(getattr(msg, "param7", 0.0)),
            }

            # ---- NEW: rule-based ACK ----
            result, reason= rule_based_ack(cmd, params, self.state)

            # ack_msg = self.mav_out.command_ack_encode(cmd, result) #commented for ack handling
            # self.send_mav(ack_msg) #commented for ack handling
            # ack_msg = self.mav_out.command_ack_encode(
            #     cmd, result,
            #     0,  # progress
            #     0,  # result_param2
            #     ts, # target_system
            #     tc  # target_component
            # )
            # ack_msg = self.mav_out.command_ack_encode(cmd, result)
            # self.send_mav(ack_msg)

            # //// delete below if fails
            try:
                # old pymavlink signature (only command, result)
                ack_msg = self.mav_out.command_ack_encode(int(cmd), int(result))
            except TypeError:
                # newer message shape with target fields
                ack_msg = mavutil.mavlink.MAVLink_command_ack_message(
                    int(cmd), int(result), 0, 0, int(ts), int(tc)
                )
            self.send_mav(ack_msg)
            # //// delete above if fails


            #  pending: log the ack as well:


            # log command into history (so LLM sees it next time)
            self.hist.add_cmd(cmd, params)

            # minimal runtime log (easy to see in terminal)
            print(f"[CMD RX] id={cmd} params={params}", flush=True)

            # optional persistent log (recommended)
            try:
                with open("cmd_rx_log.jsonl", "a") as f:
                    f.write(json.dumps({"ts": time.time(), "cmd": cmd, "params": params}) + "\n")
            except Exception as e:
                print(f"[CMD LOG ERROR] {e}", flush=True)

            # # if LLM disabled, immediately UNSUPPORTED (or ACCEPTED if you prefer)
            # if not getattr(self, "llm_enabled", False):
            #     ack_msg = self.mav_out.command_ack_encode(cmd, mavutil.mavlink.MAV_RESULT_UNSUPPORTED)
            #     self.send_mav(ack_msg)
            #     return

            resp_wrap = self.llm_prompt_command(cmd, params)

            # commented bcz it push the ack from LLM.
            # if resp_wrap and resp_wrap.get("_parsed"):
            #     self.log_cmd_vs_llm(cmd, params, resp_wrap.get("_raw",""), resp_wrap["_parsed"])
            #     self.apply_llm_command_result(resp_wrap["_parsed"])
            # else:
            #     ack_msg = self.mav_out.command_ack_encode(cmd, mavutil.mavlink.MAV_RESULT_UNSUPPORTED)
            #     self.send_mav(ack_msg)
            #     print(f"[LLM CMD FAIL] id={cmd} -> UNSUPPORTED", flush=True)
            
            # return
            # commented bcz it push the ack from LLM.

            # fixed with the static ack.
            # 4) write one combined log row (rule ack + llm ack)
            os.makedirs("./logs", exist_ok=True)
            llm_ack = (resp_wrap.get("_parsed") or {}).get("ack", {}) if resp_wrap else {}
            with open("./logs/cmd_ack_llm_log.jsonl", "a") as f:
                f.write(json.dumps({
                    "ts": time.time(),
                    "cmd": int(cmd),
                    "params": params,
                    "ack_rule": {"result_int": int(result), "reason": reason},
                    "ack_llm": llm_ack,
                    "llm_raw": resp_wrap.get("_raw","") if resp_wrap else "",
                    "llm_parsed": resp_wrap.get("_parsed") if resp_wrap else None
                }) + "\n")

            # 5) apply telemetry scheduling only
            if resp_wrap and resp_wrap.get("_parsed"):
                self.apply_llm_command_result(resp_wrap["_parsed"])
            else:
                print(f"[LLM CMD FAIL] id={cmd} (no telemetry scheduled)", flush=True)

            return

        # ------------------------------------------------------------
        # Other message types (optional: track or ignore)
        # ------------------------------------------------------------
        return



# ////////////// telemetry /////////////////////



# commenting for static heartbeat testing ////////////////////////

        # if self.llm_enabled:
        #     response = self.llm_prompt(attacker_msg)

        #     if response and self.verify_response(response):
        #         self.llm_logging(attacker_msg, response)
        #         self.llm_output_process(response)
# commenting for static heartbeat testing ////////////////////////



    # ---------------------------
    # run()  (Main loop)
    # ---------------------------
    def run(self):

        # threading.Thread(target=self.heartbeat_loop, daemon=True).start()


# ///////////// HEARTBEAT /////////////////////////
        # 1️⃣ Initialize identity
        self.initialize_heartbeat_identity()

        # 2️⃣ Start heartbeat thread
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
# ///////////// HEARTBEAT /////////////////////////


        # 3️⃣ Start request-driven telemetry scheduler
        self.telemetry_thread.start()

        while True:
            try:
                data, addr = self.sock.recvfrom(4096)
                print("[RX UDP addr]", addr, "len=", len(data), flush=True) ##debug dlt later
            except socket.timeout:
                continue

            # GCS detection
            if self.gcs_addr is None:
                self.gcs_addr = addr
                print(f"[GCS CONNECTED] {self.gcs_addr}")

            # MAVLink parsing
            for byte in data:
                msg = self.mav_in.parse_char(bytes([byte]))
                if msg:
                    self.handle_inbound_msg(msg)


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    hp = LLMHoneypot()
    hp.run()
