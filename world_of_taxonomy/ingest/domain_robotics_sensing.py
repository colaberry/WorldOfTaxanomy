"""Robotics Sensing and Perception Technology Types domain taxonomy ingester.

Classifies the sensing and perception technologies that enable robot autonomy.
Orthogonal to application domain and robot platform type.
Used by robotics engineers, sensor OEMs, systems integrators, and
investors evaluating robotics technology stacks and IP positions.

Code prefix: drbsen_
System ID: domain_robotics_sensing
Hand-coded. Public domain.
"""
from __future__ import annotations

from typing import Optional

# (code, title, level, parent_code)
ROBOTICS_SENSING_NODES: list[tuple[str, str, int, Optional[str]]] = [
    ("drbsen_vision", "Computer Vision and Imaging Systems", 1, None),
    ("drbsen_vision_rgb", "RGB cameras and monocular depth estimation", 2, "drbsen_vision"),
    ("drbsen_vision_stereo", "Stereo vision systems (depth from disparity)", 2, "drbsen_vision"),
    ("drbsen_vision_tof", "Time-of-flight 3D imaging (structured light, ToF chips)", 2, "drbsen_vision"),
    ("drbsen_vision_thermal", "Thermal and infrared imaging for detection", 2, "drbsen_vision"),
    ("drbsen_lidar", "LiDAR and Radar Sensing", 1, None),
    ("drbsen_lidar_mech", "Mechanical spinning LiDAR (Velodyne, HESAI multi-layer)", 2, "drbsen_lidar"),
    ("drbsen_lidar_solid", "Solid-state LiDAR (MEMS, OPA, Flash LiDAR)", 2, "drbsen_lidar"),
    ("drbsen_lidar_radar", "Automotive radar (77 GHz FMCW for pedestrian/object detection)", 2, "drbsen_lidar"),
    ("drbsen_force", "Force, Torque and Tactile Sensing", 1, None),
    ("drbsen_force_ft", "6-axis force/torque sensors (assembly contact detection)", 2, "drbsen_force"),
    ("drbsen_force_tactile", "Tactile skin arrays (grasping, surface texture sensing)", 2, "drbsen_force"),
    ("drbsen_force_joint", "Joint torque sensors (collaborative robots, safety monitoring)", 2, "drbsen_force"),
    ("drbsen_proprioceptive", "Proprioceptive and Navigation Sensing", 1, None),
    ("drbsen_proprioceptive_imu", "IMU (accelerometer, gyroscope, magnetometer) for pose estimation", 2, "drbsen_proprioceptive"),
    ("drbsen_proprioceptive_encoder", "Rotary/linear encoders for joint position feedback", 2, "drbsen_proprioceptive"),
    ("drbsen_proprioceptive_gnss", "GPS/GNSS with RTK for outdoor robot localization", 2, "drbsen_proprioceptive"),
]

_DOMAIN_ROW = (
    "domain_robotics_sensing",
    "Robotics Sensing and Perception Technology Types",
    "Sensing and perception technology classification for robots and autonomous systems",
    "WorldOfTaxonomy",
    None,
)

# NAICS prefixes to link
_NAICS_PREFIXES = ['3332', '3339', '3345']


def _determine_level(code: str) -> int:
    """Return 1 for top categories, 2 for specific types."""
    return 1 if len(code.split("_")) == 2 else 2


def _determine_parent(code: str) -> Optional[str]:
    """Return parent category code, or None for top-level categories."""
    parts = code.split("_")
    if len(parts) <= 2:
        return None
    return "_".join(parts[:2])


async def ingest_domain_robotics_sensing(conn) -> int:
    """Ingest Robotics Sensing and Perception Technology Types domain taxonomy.

    Registers in domain_taxonomy, stores nodes in classification_node
    (system_id='domain_robotics_sensing'), and links NAICS nodes via node_taxonomy_link.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "domain_robotics_sensing",
        "Robotics Sensing and Perception Technology Types",
        "Sensing and perception technology classification for robots and autonomous systems",
        "1.0",
        "Global",
        "WorldOfTaxonomy",
    )

    await conn.execute(
        """INSERT INTO domain_taxonomy
               (id, name, full_name, authority, url, code_count)
           VALUES ($1, $2, $3, $4, $5, 0)
           ON CONFLICT (id) DO UPDATE SET code_count = 0""",
        *_DOMAIN_ROW,
    )

    parent_codes = {parent for _, _, _, parent in ROBOTICS_SENSING_NODES if parent is not None}

    rows = [
        (
            "domain_robotics_sensing",
            code,
            title,
            level,
            parent,
            code.split("_")[1],
            code not in parent_codes,
        )
        for code, title, level, parent in ROBOTICS_SENSING_NODES
    ]

    await conn.executemany(
        """INSERT INTO classification_node
               (system_id, code, title, level, parent_code, sector_code, is_leaf)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (system_id, code) DO NOTHING""",
        rows,
    )

    count = len(ROBOTICS_SENSING_NODES)

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'domain_robotics_sensing'",
        count,
    )
    await conn.execute(
        "UPDATE domain_taxonomy SET code_count = $1 WHERE id = 'domain_robotics_sensing'",
        count,
    )

    naics_codes = [
        row["code"]
        for prefix in _NAICS_PREFIXES
        for row in await conn.fetch(
            "SELECT code FROM classification_node "
            "WHERE system_id = 'naics_2022' AND code LIKE $1",
            prefix + "%",
        )
    ]

    if naics_codes:
        await conn.executemany(
            """INSERT INTO node_taxonomy_link
                   (system_id, node_code, taxonomy_id, relevance)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (system_id, node_code, taxonomy_id) DO NOTHING""",
            [("naics_2022", code, "domain_robotics_sensing", "primary") for code in naics_codes],
        )

    return count
