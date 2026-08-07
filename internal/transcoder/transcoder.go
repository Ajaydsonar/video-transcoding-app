// Package transcoder wraps the ffmpeg/ffprobe binaries. Everything about
// *how* we talk to ffmpeg lives here — nothing outside this package
// should know or care that a subprocess is involved at all.
package transcoder

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

// Rendition describes one output in the resolution ladder. ShortEdge
// targets whichever dimension is smaller on the SOURCE video — height for
// landscape, width for portrait — so the ladder works correctly for both
// orientations without ever distorting the image.
type Rendition struct {
	Name      string // used as the output filename, e.g. "480p" -> "480p.mp4"
	ShortEdge int
	Bitrate   string // ffmpeg bitrate string, e.g. "1400k"
}

// DefaultLadder is deliberately small so local testing doesn't take
// forever. Each rung is only actually rendered if the source's own short
// edge is at least that big — see Transcode.
var DefaultLadder = []Rendition{
	{Name: "1080p", ShortEdge: 1080, Bitrate: "5000k"},
	{Name: "720p", ShortEdge: 720, Bitrate: "2800k"},
	{Name: "480p", ShortEdge: 480, Bitrate: "1400k"},
	{Name: "360p", ShortEdge: 360, Bitrate: "800k"},
}

type FFmpeg struct {
	Bin      string
	ProbeBin string
}

func New() *FFmpeg {
	return &FFmpeg{Bin: "ffmpeg", ProbeBin: "ffprobe"}
}

// SourceInfo is what we need to know about a video BEFORE deciding how to
// transcode it: how long it is (for progress %), and its shape (for
// picking a distortion-free scale and skipping upscale-only renditions).
type SourceInfo struct {
	Duration float64
	Width    int
	Height   int
}

type probeOutput struct {
	Streams []struct {
		Width  int `json:"width"`
		Height int `json:"height"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

// Probe asks ffprobe for everything we need in one call: JSON output
// merges the "stream" section (dimensions) and "format" section
// (duration) into one document, which is far less fragile to parse than
// gluing together two separate CSV calls.
func (f *FFmpeg) Probe(ctx context.Context, inputPath string) (SourceInfo, error) {
	cmd := exec.CommandContext(ctx, f.ProbeBin,
		"-v", "error",
		"-select_streams", "v:0",
		"-show_entries", "stream=width,height:format=duration",
		"-of", "json",
		inputPath,
	)
	out, err := cmd.Output()
	if err != nil {
		return SourceInfo{}, fmt.Errorf("transcoder: probing source: %w", err)
	}

	var parsed probeOutput
	if err := json.Unmarshal(out, &parsed); err != nil {
		return SourceInfo{}, fmt.Errorf("transcoder: parsing ffprobe output: %w", err)
	}
	if len(parsed.Streams) == 0 {
		return SourceInfo{}, fmt.Errorf("transcoder: no video stream found in %s", inputPath)
	}
	duration, err := strconv.ParseFloat(parsed.Format.Duration, 64)
	if err != nil {
		return SourceInfo{}, fmt.Errorf("transcoder: parsing duration %q: %w", parsed.Format.Duration, err)
	}

	return SourceInfo{
		Duration: duration,
		Width:    parsed.Streams[0].Width,
		Height:   parsed.Streams[0].Height,
	}, nil
}

// planRenditions decides which ladder rungs actually get encoded, and
// what ffmpeg scale-filter value each needs, based on the source's real
// shape. Never upscales; always renders at least one output.
func planRenditions(src SourceInfo, ladder []Rendition) []Rendition {
	landscape := src.Width >= src.Height
	shortEdge := src.Height
	if !landscape {
		shortEdge = src.Width
	}

	var picked []Rendition
	for _, r := range ladder {
		if r.ShortEdge <= shortEdge {
			picked = append(picked, r)
		}
	}
	if len(picked) == 0 {
		// source is smaller than every rung in the ladder — encode at
		// its native size instead of producing zero outputs
		picked = []Rendition{{Name: "source", ShortEdge: shortEdge, Bitrate: "1400k"}}
	}
	return picked
}

// RenditionResult describes one file Transcode actually produced —
// its real dimensions, as measured from the output file itself rather
// than recomputed in Go, since ffmpeg's -2 auto-dimension rounding means
// the true output isn't always the exact number you'd expect by hand.
type RenditionResult struct {
	Name   string
	Width  int
	Height int
}

func scaleFilter(landscape bool, shortEdge int) string {
	if landscape {
		return fmt.Sprintf("-2:%d", shortEdge) // pin height, auto width
	}
	return fmt.Sprintf("%d:-2", shortEdge) // pin width, auto height
}

// Transcode decodes inputPath ONCE and encodes it into every rendition in
// ladder, writing each as "<outDir>/<rendition.Name>.mp4". onProgress is
// called with 0-100 as ffmpeg reports how far through the source it's got.
//
// -progress pipe:1 makes ffmpeg emit machine-readable "key=value" lines
// to stdout as it works (instead of the human progress bar, which goes to
// stderr and is annoying to parse reliably). We read those lines live,
// while ffmpeg is still running, and translate them into percentages.
func (f *FFmpeg) Transcode(ctx context.Context, inputPath, outDir string, ladder []Rendition, onProgress func(percent int)) ([]RenditionResult, error) {
	src, err := f.Probe(ctx, inputPath)
	if err != nil {
		return nil, err
	}
	landscape := src.Width >= src.Height
	toRender := planRenditions(src, ladder)

	args := []string{
		"-y", // overwrite output files without prompting
		"-i", inputPath,
		"-progress", "pipe:1",
		"-nostats",
	}
	// One -map/-vf/-c:v/-c:a/output group PER rendition. ffmpeg decodes
	// the input once and feeds every group from that single decode —
	// far cheaper than running ffmpeg N separate times.
	for _, r := range toRender {
		args = append(args,
			"-map", "0:v", "-map", "0:a?",
			"-vf", "scale="+scaleFilter(landscape, r.ShortEdge),
			"-c:v", "libx264", "-preset", "veryfast", "-b:v", r.Bitrate,
			"-c:a", "aac", "-b:a", "128k",
			fmt.Sprintf("%s/%s.mp4", outDir, r.Name),
		)
	}

	cmd := exec.CommandContext(ctx, f.Bin, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("transcoder: attaching stdout: %w", err)
	}
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf // ffmpeg's real error output, kept only for the error message if it fails

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("transcoder: starting ffmpeg: %w", err)
	}

	// Read progress lines until ffmpeg closes stdout (i.e. until it
	// exits). This MUST happen before cmd.Wait() below — os/exec
	// requires you to finish reading a pipe before waiting, or you can
	// deadlock if ffmpeg fills its output buffer with nobody draining it.
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "=")
		if !found || key != "out_time_us" || src.Duration <= 0 {
			continue
		}
		microseconds, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			continue // one malformed line shouldn't fail the whole job
		}
		percent := int((float64(microseconds) / 1_000_000 / src.Duration) * 100)
		if percent > 100 {
			percent = 100
		}
		if percent >= 0 {
			onProgress(percent)
		}
	}

	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("transcoder: ffmpeg failed: %w (stderr: %s)", err, stderrBuf.String())
	}
	onProgress(100)

	// ffmpeg succeeded — probe each file it actually wrote to report its
	// REAL dimensions back to the caller, rather than trusting our own
	// scale-filter math to have predicted them exactly.
	results := make([]RenditionResult, 0, len(toRender))
	for _, r := range toRender {
		outPath := fmt.Sprintf("%s/%s.mp4", outDir, r.Name)
		info, err := f.Probe(ctx, outPath)
		if err != nil {
			return nil, fmt.Errorf("transcoder: probing output %s: %w", r.Name, err)
		}
		results = append(results, RenditionResult{Name: r.Name, Width: info.Width, Height: info.Height})
	}

	return results, nil
}
