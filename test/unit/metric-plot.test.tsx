// @vitest-environment jsdom

import "@testing-library/jest-dom/vitest";

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { MetricPlot } from "../../src/features/kafka/ui/MetricPlot";

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("bounded metric plot", () => {
  it("renders finite labelled series, omits unavailable points and creates no timers", () => {
    const interval = vi.spyOn(globalThis, "setInterval");
    const timeout = vi.spyOn(globalThis, "setTimeout");

    render(
      <MetricPlot
        sampleLabels={[
          "2026-08-01T12:00:00.000Z",
          "2026-08-01T12:01:00.000Z",
          "2026-08-01T12:02:00.000Z",
          "2026-08-01T12:03:00.000Z",
        ]}
        series={[
          { label: "Average", values: [7, null, 6, 5] },
          { label: "P95", values: [9, null, 8, 6] },
        ]}
        title="Publish-to-observe latency trend"
        unit="ms"
      />,
    );

    const plot = screen.getByRole("region", { name: /Publish-to-observe latency trend/u });
    expect(plot).toHaveAccessibleName(
      /Average: 3 samples, latest 5 ms.*P95: 3 samples, latest 6 ms/u,
    );
    expect(plot.querySelectorAll("path")).toHaveLength(2);
    expect(plot.querySelectorAll("path")[0]).toHaveAttribute("d", expect.not.stringContaining("Q"));
    expect(plot.querySelectorAll("path")[0]).toHaveAttribute("d", expect.stringContaining("L"));
    expect(plot.querySelectorAll("path")[0]?.getAttribute("d")?.match(/M /gu)).toHaveLength(2);
    expect(screen.getByLabelText("Range maximum 9 ms")).toBeVisible();
    expect(screen.getByLabelText("Range minimum 5 ms")).toBeVisible();
    const exactSamples = screen.getAllByRole("listitem", {
      name: /at 2026-08-01T12:0[0-3]:00.000Z/u,
    });
    expect(exactSamples).toHaveLength(6);
    expect(exactSamples[0]).toHaveAccessibleName("Average at 2026-08-01T12:00:00.000Z: 7 ms");
    expect(
      screen.getByRole("group", { name: "Publish-to-observe latency trend plot" }),
    ).toHaveAttribute("tabindex", "0");
    expect(exactSamples.every((point) => point.getAttribute("tabindex") !== "0")).toBe(true);
    expect(plot).toHaveTextContent("12:00:00");
    expect(plot).toHaveTextContent("12:03:00");
    expect(exactSamples[0]).toHaveTextContent("Average at 2026-08-01T12:00:00.000Z: 7 ms");
    expect(plot.innerHTML).not.toContain("NaN");
    expect(interval).not.toHaveBeenCalled();
    expect(timeout).not.toHaveBeenCalled();
  });

  it("spaces irregular timestamps proportionally and identifies missing latest evidence", () => {
    render(
      <MetricPlot
        sampleLabels={[
          "2026-09-14T00:00:00Z",
          "2026-09-14T00:00:01Z",
          "2026-09-14T00:01:00Z",
          "2026-09-14T00:02:00Z",
        ]}
        series={[{ label: "Latency", values: [10, 20, 15, null] }]}
        title="Probe"
        unit="ms"
      />,
    );
    const samples = screen.getAllByRole("listitem");
    const x = samples.map((p) => Number(p.getAttribute("cx")));
    expect((x[2]! - x[0]!) / (x[1]! - x[0]!)).toBeCloseTo(60);
    expect(screen.getByRole("region", { name: /Probe/u })).toHaveAccessibleName(
      /latest Unavailable.*last measured 15 ms at 2026-09-14T00:01:00Z/u,
    );
    expect(screen.getByText("Time (UTC)")).toBeVisible();
  });

  it("uses one stable keyboard entry and traverses exact samples as history changes", () => {
    const props = {
      title: "Probe",
      unit: "ms",
      sampleLabels: ["first", "second"],
      series: [{ label: "Latency", values: [10, 20] }],
    };
    const view = render(<MetricPlot {...props} />);
    const plot = screen.getByRole("group", { name: "Probe plot" });
    expect(plot).toHaveAttribute("tabindex", "0");
    plot.focus();
    fireEvent.keyDown(plot, { key: "ArrowRight" });
    expect(screen.getByRole("status")).toHaveTextContent("Latency at second: 20 ms");
    view.rerender(
      <MetricPlot
        {...props}
        sampleLabels={["second", "third"]}
        series={[{ label: "Latency", values: [20, 30] }]}
      />,
    );
    expect(plot).toHaveFocus();
    expect(screen.getByRole("status")).toHaveTextContent("Latency at second: 20 ms");
    fireEvent.keyDown(plot, { key: "End" });
    expect(screen.getByRole("status")).toHaveTextContent("Latency at third: 30 ms");
    fireEvent.keyDown(plot, { key: "Home" });
    expect(screen.getByRole("status")).toHaveTextContent("Latency at second: 20 ms");
    expect(screen.getByText("Sample sequence")).toBeVisible();
  });

  it("states when no finite trend exists instead of drawing a healthy zero line", () => {
    render(
      <MetricPlot
        sampleLabels={["12:00", "12:01"]}
        series={[{ label: "Delivery", values: [null, null] }]}
        title="Delivery rate trend"
        unit="msg/s"
      />,
    );

    expect(screen.getByText("No measured samples")).toBeVisible();
    expect(screen.queryByRole("region", { name: /Delivery rate trend/u })).not.toBeInTheDocument();
  });

  it("preserves series color identity when an earlier series is entirely unavailable", () => {
    render(
      <MetricPlot
        title="Rates"
        unit="msg/s"
        sampleLabels={["first", "second"]}
        series={[
          { label: "Unavailable series", values: [null, null] },
          { label: "Measured series", values: [2, 3] },
        ]}
      />,
    );
    const plot = screen.getByRole("group", { name: "Rates plot" });
    expect(plot.querySelectorAll("path")).toHaveLength(1);
    expect(plot.querySelector("path")).toHaveAttribute(
      "stroke",
      "var(--streamskope-plot-secondary)",
    );
    expect(plot.querySelector("path")).toHaveAttribute("stroke-dasharray", "6 4");
    expect(screen.getByRole("region", { name: /Rates/u })).toHaveAccessibleName(
      /Unavailable series: 0 samples, latest Unavailable/u,
    );
  });

  it("draws a constant zero series on the baseline instead of implying a mid-scale value", () => {
    render(
      <MetricPlot
        interpolation="step"
        sampleLabels={["2026-08-01T12:00:00.000Z", "2026-08-01T12:01:00.000Z"]}
        series={[{ label: "Queue", values: [0, 0] }]}
        title="Queue depth trend"
        unit="messages"
      />,
    );

    expect(
      screen.getByRole("region", { name: /Queue depth trend/u }).querySelector("path"),
    ).toHaveAttribute("d", expect.stringContaining("102"));
    expect(
      screen.getByRole("region", { name: /Queue depth trend/u }).querySelector("path"),
    ).toHaveAttribute("d", expect.stringMatching(/H .* V /u));
  });
});
