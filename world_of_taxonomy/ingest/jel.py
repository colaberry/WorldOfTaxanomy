"""JEL Codes (Journal of Economic Literature Classification System) ingester.

The JEL classification system is the standard classification for
economics research, maintained by the American Economic Association.
Organizes economics into 20 top categories and ~100 sub-categories.

Source: American Economic Association (aeaweb.org)
License: Freely available in the public domain
"""
from __future__ import annotations

JEL_MAIN: list[tuple[str, str]] = [
    ("A", "General Economics and Teaching"),
    ("B", "Schools of Economic Thought and Methodology"),
    ("C", "Mathematical and Quantitative Methods"),
    ("D", "Microeconomics"),
    ("E", "Macroeconomics and Monetary Economics"),
    ("F", "International Economics"),
    ("G", "Financial Economics"),
    ("H", "Public Economics"),
    ("I", "Health, Education, and Welfare"),
    ("J", "Labor and Demographic Economics"),
    ("K", "Law and Economics"),
    ("L", "Industrial Organization"),
    ("M", "Business Administration and Business Economics"),
    ("N", "Economic History"),
    ("O", "Economic Development, Innovation, Technological Change and Growth"),
    ("P", "Economic Systems"),
    ("Q", "Agricultural and Natural Resource Economics; Environmental Economics"),
    ("R", "Urban, Rural, Regional, Real Estate and Transportation Economics"),
    ("Y", "Miscellaneous Categories"),
    ("Z", "Other Special Topics"),
]

JEL_SUBCODES: list[tuple[str, str, str]] = [
    ("A1", "General Economics", "A"),
    ("A2", "Economic Education and Teaching of Economics", "A"),
    ("A3", "Collective Works", "A"),
    ("C0", "General Econometric Tools", "C"),
    ("C1", "Econometric and Statistical Methods and Methodology: General", "C"),
    ("C2", "Single Equation Models; Multiple Variables", "C"),
    ("C3", "Multiple or Simultaneous Equation Models", "C"),
    ("C4", "Econometric and Statistical Methods: Special Topics", "C"),
    ("C5", "Econometric Modeling", "C"),
    ("C6", "Mathematical Methods and Programming", "C"),
    ("C7", "Game Theory and Bargaining Theory", "C"),
    ("C8", "Data Collection and Data Estimation Methodology; Computer Programs", "C"),
    ("C9", "Design of Experiments", "C"),
    ("D0", "General Microeconomics", "D"),
    ("D1", "Household Behavior and Family Economics", "D"),
    ("D2", "Production and Organizations", "D"),
    ("D3", "Distribution", "D"),
    ("D4", "Market Structure, Pricing and Design", "D"),
    ("D5", "General Equilibrium and Disequilibrium", "D"),
    ("D6", "Welfare Economics", "D"),
    ("D7", "Analysis of Collective Decision-Making", "D"),
    ("D8", "Information, Knowledge, and Uncertainty", "D"),
    ("D9", "Micro-Based Behavioral Economics", "D"),
    ("E0", "General Macroeconomics and Monetary Economics", "E"),
    ("E1", "General Aggregative Models", "E"),
    ("E2", "Consumption, Saving, Production, Employment and Investment", "E"),
    ("E3", "Prices, Business Fluctuations and Cycles", "E"),
    ("E4", "Money and Interest Rates", "E"),
    ("E5", "Monetary Policy, Central Banking, and the Supply of Money and Credit", "E"),
    ("E6", "Macroeconomic Policy, Macroeconomic Aspects of Public Finance", "E"),
    ("E7", "Macro-Based Behavioral Economics", "E"),
    ("F0", "General International Economics", "F"),
    ("F1", "Trade", "F"),
    ("F2", "International Factor Movements and International Business", "F"),
    ("F3", "International Finance", "F"),
    ("F4", "Macroeconomic Aspects of International Trade and Finance", "F"),
    ("F5", "International Relations, National Security and International Political Economy", "F"),
    ("F6", "Economic Impacts of Globalization", "F"),
    ("G0", "General Financial Economics", "G"),
    ("G1", "General Financial Markets", "G"),
    ("G2", "Financial Institutions and Services", "G"),
    ("G3", "Corporate Finance and Governance", "G"),
    ("G4", "Behavioral Finance", "G"),
    ("G5", "Household Finance", "G"),
    ("H0", "General Public Economics", "H"),
    ("H1", "Structure and Scope of Government", "H"),
    ("H2", "Taxation, Subsidies and Revenue", "H"),
    ("H3", "Fiscal Policies and Behavior of Economic Agents", "H"),
    ("H4", "Publicly Provided Goods", "H"),
    ("H5", "National Government Expenditures and Related Policies", "H"),
    ("H6", "National Budget, Deficit and Debt", "H"),
    ("H7", "State and Local Government; Intergovernmental Relations", "H"),
    ("H8", "Miscellaneous Public Economics Issues", "H"),
    ("I0", "General Health, Education and Welfare", "I"),
    ("I1", "Health", "I"),
    ("I2", "Education and Research Institutions", "I"),
    ("I3", "Welfare, Well-Being and Poverty", "I"),
    ("J0", "General Labor Economics", "J"),
    ("J1", "Demographic Economics", "J"),
    ("J2", "Demand and Supply of Labor", "J"),
    ("J3", "Wages, Compensation and Labor Costs", "J"),
    ("J4", "Particular Labor Markets", "J"),
    ("J5", "Labor-Management Relations, Trade Unions and Collective Bargaining", "J"),
    ("J6", "Mobility, Unemployment, Vacancies and Immigrant Workers", "J"),
    ("J7", "Labor Discrimination", "J"),
    ("J8", "Labor Standards: National and International", "J"),
    ("O0", "General Economic Development", "O"),
    ("O1", "Economic Development", "O"),
    ("O2", "Development Planning and Policy", "O"),
    ("O3", "Innovation; Research and Development; Technological Change", "O"),
    ("O4", "Economic Growth and Aggregate Productivity", "O"),
    ("O5", "Economywide Country Studies", "O"),
    ("Q0", "General Agricultural and Natural Resource Economics", "Q"),
    ("Q1", "Agriculture", "Q"),
    ("Q2", "Renewable Resources and Conservation", "Q"),
    ("Q3", "Nonrenewable Resources and Conservation", "Q"),
    ("Q4", "Energy", "Q"),
    ("Q5", "Environmental Economics", "Q"),
]


async def ingest_jel(conn) -> int:
    """Ingest JEL classification codes into classification_system and classification_node.

    Returns total node count.
    """
    await conn.execute(
        """INSERT INTO classification_system
               (id, name, full_name, version, region, authority, node_count)
           VALUES ($1, $2, $3, $4, $5, $6, 0)
           ON CONFLICT (id) DO UPDATE SET node_count = 0""",
        "jel",
        "JEL Codes",
        "Journal of Economic Literature Classification System",
        "2020",
        "Global",
        "American Economic Association",
    )

    count = 0
    for seq, (code, title) in enumerate(JEL_MAIN, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "jel", code, title, 1, None, code, False, seq,
        )
        count += 1

    for seq, (code, title, parent) in enumerate(JEL_SUBCODES, 1):
        await conn.execute(
            """INSERT INTO classification_node
                   (system_id, code, title, level, parent_code, sector_code, is_leaf, seq_order)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
               ON CONFLICT (system_id, code) DO NOTHING""",
            "jel", code, title, 2, parent, parent, True, seq,
        )
        count += 1

    await conn.execute(
        "UPDATE classification_system SET node_count = $1 WHERE id = 'jel'",
        count,
    )

    return count
