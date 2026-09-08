"""Validate an fastEHC .xlsx beyond mere XML well-formedness.

Checks the things Excel's strict validator rejects but openpyxl/ElementTree happily
accept -- most importantly chart axis reference integrity (a dangling crossAx or
axId makes Excel discard the entire drawing part, silently taking every chart and
image on that sheet with it).
"""
import re
import sys
import xml.etree.ElementTree as ET
import zipfile

C = "{http://schemas.openxmlformats.org/drawingml/2006/chart}"
AXIS_TAGS = ("catAx", "valAx", "dateAx", "serAx")
PLOT_TAGS = ("barChart", "lineChart", "pieChart", "pie3DChart", "areaChart",
             "scatterChart", "doughnutChart", "radarChart", "bubbleChart")


def validate(path):
    problems = []
    z = zipfile.ZipFile(path)
    names = z.namelist()

    bad = z.testzip()
    if bad:
        problems.append(f"zip: corrupt entry {bad}")

    for name in names:
        if name.endswith(".xml") or name.endswith(".rels"):
            try:
                ET.fromstring(z.read(name))
            except ET.ParseError as e:
                problems.append(f"{name}: not well-formed: {e}")

    # Relationship targets must exist
    for name in names:
        if not name.endswith(".rels"):
            continue
        base = "" if name == "_rels/.rels" else name.rsplit("/_rels/", 1)[0]
        for target in re.findall(r'Target="([^"]+)"', z.read(name).decode("utf-8")):
            if target.startswith(("http://", "https://", "mailto:", "../printerSettings")):
                continue
            if target.startswith("/"):
                resolved = target[1:]
            else:
                resolved = (f"{base}/{target}" if base else target).replace("/./", "/")
            while "/../" in resolved:
                resolved = re.sub(r"[^/]+/\.\./", "", resolved, count=1)
            if resolved not in names:
                problems.append(f"{name}: dangling relationship target {target!r} -> {resolved}")

    # Chart axis reference integrity
    chart_parts = sorted(n for n in names if re.match(r"xl/charts/chart\d+\.xml$", n))
    for name in chart_parts:
        root = ET.fromstring(z.read(name))
        plot = root.find(f".//{C}plotArea")
        if plot is None:
            problems.append(f"{name}: no plotArea")
            continue

        defined = {}
        for tag in AXIS_TAGS:
            for ax in plot.findall(f"{C}{tag}"):
                el = ax.find(f"{C}axId")
                if el is None:
                    problems.append(f"{name}: <{tag}> with no axId")
                    continue
                defined[el.get("val")] = tag

        referenced = set()
        for tag in PLOT_TAGS:
            for grp in plot.findall(f"{C}{tag}"):
                for el in grp.findall(f"{C}axId"):
                    referenced.add(el.get("val"))

        for ref in sorted(referenced):
            if ref not in defined:
                problems.append(
                    f"{name}: plot group references axId {ref} which is not defined "
                    f"(defined: {sorted(defined)})")

        for tag in AXIS_TAGS:
            for ax in plot.findall(f"{C}{tag}"):
                axid_el = ax.find(f"{C}axId")
                cross_el = ax.find(f"{C}crossAx")
                axid = axid_el.get("val") if axid_el is not None else "?"
                if cross_el is None:
                    problems.append(f"{name}: <{tag} axId={axid}> has no crossAx")
                    continue
                cross = cross_el.get("val")
                if cross not in defined:
                    problems.append(
                        f"{name}: <{tag} axId={axid}> crossAx={cross} points at an "
                        f"axis that does not exist (defined: {sorted(defined)})")
                elif cross == axid:
                    problems.append(f"{name}: <{tag} axId={axid}> crossAx points at itself")

    return problems, len(chart_parts)


if __name__ == "__main__":
    target = sys.argv[1]
    problems, n_charts = validate(target)
    print(f"Validated {target}")
    print(f"  chart parts: {n_charts}")
    if problems:
        print(f"  PROBLEMS ({len(problems)}):")
        for p in problems:
            print(f"    - {p}")
        sys.exit(1)
    print("  OK: no structural problems found")
