// Package transcoder
package transcoder

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

// HLS constants controlling the packaged output. SegmentSeconds is the
// target duration of every media segment and the interval at which every
// video rendition is forced to place a keyframe, so all variants share
// aligned segment boundaries and a player can switch renditions cleanly.
const (
	SegmentSeconds     = 6
	MasterPlaylistName = "master.m3u8"
)

// HLSVariant describes one packaged rendition produced by TranscodeHLS —
// the info the worker needs to fill in a job.Output (Bandwidth is the
// actual bitrate measured from the files that were written, not the
// encoder target).
type HLSVariant struct {
	Name      string `json:"name"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
	Bandwidth int    `json:"bandwidth"`
	Segments  int    `json:"segments"`
	Bytes     int64  `json:"bytes"`
	HasAudio  bool   `json:"hasAudio,omitempty"`
}

// hlsSource is everything TranscodeHLS needs to know about the upload
// before encoding: duration (for progress %), shape (for distortion-free
// scaling and orientation), fps (for a GOP sized to the segment length),
// and whether the source has any audio track.
type hlsSource struct {
	Duration float64
	Width    int
	Height   int
	FPS      float64
	HasAudio bool
}

// probeHLSInfo asks ffprobe for duration, dimensions, frame rate and audio
// presence in one JSON call. Kept separate from Probe so the MP4 pipeline's
// structs stay untouched; dimension/fps probing also needs the stream list
// rather than a single selected stream.
func (f *FFmpeg) probeHLSInfo(ctx context.Context, inputPath string) (hlsSource, error) {
	cmd := exec.CommandContext(ctx, f.ProbeBin,
		"-v", "error",
		"-show_entries", "stream=codec_type,width,height,avg_frame_rate,r_frame_rate:format=duration",
		"-of", "json",
		inputPath,
	)
	out, err := cmd.Output()
	if err != nil {
		return hlsSource{}, fmt.Errorf("transcoder: probing source for HLS: %w", err)
	}

	var parsed struct {
		Streams []struct {
			CodecType    string `json:"codec_type"`
			Width        int    `json:"width"`
			Height       int    `json:"height"`
			AvgFrameRate string `json:"avg_frame_rate"`
			RFrameRate   string `json:"r_frame_rate"`
		} `json:"streams"`
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &parsed); err != nil {
		return hlsSource{}, fmt.Errorf("transcoder: parsing HLS probe output: %w", err)
	}

	src := hlsSource{}
	for _, s := range parsed.Streams {
		switch s.CodecType {
		case "video":
			if src.Width == 0 && src.Height == 0 {
				src.Width = s.Width
				src.Height = s.Height
			}
			if src.FPS == 0 {
				src.FPS = parseFrameRate(s.AvgFrameRate, s.RFrameRate)
			}
		case "audio":
			src.HasAudio = true
		}
	}
	if src.Width == 0 || src.Height == 0 {
		return hlsSource{}, fmt.Errorf("transcoder: no video stream found in %s", inputPath)
	}

	src.Duration, err = strconv.ParseFloat(parsed.Format.Duration, 64)
	if err != nil {
		return hlsSource{}, fmt.Errorf("transcoder: parsing HLS duration %q: %w", parsed.Format.Duration, err)
	}
	return src, nil
}

// parseFrameRate turns ffprobe's "30000/1001" (or "25" or "0/0") into a
// float, preferring the average rate and falling back to the frame rate.
func parseFrameRate(avg, frame string) float64 {
	for _, s := range []string{avg, frame} {
		parts := strings.SplitN(s, "/", 2)
		num, err1 := strconv.ParseFloat(parts[0], 64)
		if err1 != nil {
			continue
		}
		if len(parts) == 1 {
			return num
		}
		den, err2 := strconv.ParseFloat(parts[1], 64)
		if err2 != nil || den <= 0 {
			continue
		}
		return num / den
	}
	return 0
}

// TranscodeHLS packages the input into a VOD HLS / fMP4 tree inside outDir:
//
//	outDir/master.m3u8
//	outDir/<rendition>/playlist.m3u8  init.mp4  segment-00000.m4s ...
//	outDir/audio/playlist.m3u8        init.mp4  segment-00000.m4s ...
//
// Each video rendition is brand-separate video-only HLS output so a single
// shared audio playlist can be referenced by every variant, exactly one
// ffmpeg decode feeds all outputs. Every rendition places a keyframe every
// SegmentSeconds, giving aligned boundaries across variants for clean ABR
// switching. onProgress reports 0-100 as ffmpeg progresses through the
// source, identical to the MP4 path.
func (f *FFmpeg) TranscodeHLS(ctx context.Context, inputPath, outDir string, ladder []Rendition, onProgress func(percent int)) ([]HLSVariant, error) {
	src, err := f.probeHLSInfo(ctx, inputPath)
	if err != nil {
		return nil, err
	}

	toRender := planRenditions(SourceInfo{
		Duration: src.Duration,
		Width:    src.Width,
		Height:   src.Height,
	}, ladder)

	landscape := src.Width >= src.Height
	gop := 0
	if src.FPS > 0 {
		gop = int(math.Round(src.FPS * SegmentSeconds))
	}

	// ffmpeg resolves -hls_fmp4_init_filename and -hls_segment_filename
	// against the variant directory, which must already exist (it does NOT
	// create it). All paths below are relative to outDir via Cmd.Dir, and
	// the generated media playlists end up with clean basename references.
	for _, r := range toRender {
		if err := os.MkdirAll(filepath.Join(outDir, hlsDir(r.Name)), 0o755); err != nil {
			return nil, fmt.Errorf("creating HLS output dir for %s: %w", r.Name, err)
		}
	}
	if src.HasAudio {
		if err := os.MkdirAll(filepath.Join(outDir, "audio"), 0o755); err != nil {
			return nil, fmt.Errorf("creating audio output dir: %w", err)
		}
	}

	args := []string{
		"-y",
		"-i", inputPath,
		"-progress", "pipe:1",
		"-nostats",
	}

	for _, r := range toRender {
		dir := hlsDir(r.Name)
		args = append(args,
			"-map", "0:v:0",
			"-vf", "scale="+scaleFilter(landscape, r.ShortEdge),
			"-c:v", "libx264",
			"-preset", "veryfast",
			"-profile:v", "main",
			"-level", "40", // L4.0 supports up to 1080p@30; see CODECS note
			"-pix_fmt", "yuv420p",
			"-b:v", r.Bitrate,
		)
		if maxrate, bufsize, ok := bitrateParams(r.Bitrate); ok {
			args = append(args, "-maxrate", maxrate, "-bufsize", bufsize)
		}
		if gop > 0 {
			args = append(args, "-g", strconv.Itoa(gop), "-keyint_min", strconv.Itoa(gop))
		}
		args = append(args,
			"-sc_threshold", "0",
			"-force_key_frames", fmt.Sprintf("expr:gte(t,n_forced*%d)", SegmentSeconds),
			"-flags", "+cgop",
			"-f", "hls",
			"-hls_time", strconv.Itoa(SegmentSeconds),
			"-hls_playlist_type", "vod",
			"-hls_segment_type", "fmp4",
			"-hls_flags", "independent_segments",
			"-hls_fmp4_init_filename", "init.mp4",
			"-hls_segment_filename", fmt.Sprintf("%s/segment-%%05d.m4s", dir),
			dir+"/playlist.m3u8",
		)
	}

	if src.HasAudio {
		args = append(args,
			"-map", "0:a:0?",
			"-c:a", "aac",
			"-b:a", "128k",
			"-ac", "2",
			"-ar", "48000",
			"-f", "hls",
			"-hls_time", strconv.Itoa(SegmentSeconds),
			"-hls_playlist_type", "vod",
			"-hls_segment_type", "fmp4",
			"-hls_flags", "independent_segments",
			"-hls_fmp4_init_filename", "init.mp4",
			"-hls_segment_filename", "audio/segment-%05d.m4s",
			"audio/playlist.m3u8",
		)
	}

	cmd := exec.CommandContext(ctx, f.Bin, args...)
	cmd.Dir = outDir

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("transcoder: attaching HLS stdout: %w", err)
	}
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("transcoder: starting HLS ffmpeg: %w", err)
	}

	// Drain stdout before Wait, or a full pipe can deadlock the process.
	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		key, value, found := strings.Cut(scanner.Text(), "=")
		if !found || key != "out_time_us" || src.Duration <= 0 {
			continue
		}
		microseconds, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			continue
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
		return nil, fmt.Errorf("transcoder: HLS ffmpeg failed: %w (stderr: %s)", err, stderrBuf.String())
	}
	onProgress(100)

	variants := make([]HLSVariant, 0, len(toRender))
	for _, r := range toRender {
		dir := hlsDir(r.Name)

		width, height, err := actualDimensions(ctx, f, filepath.Join(outDir, dir, "init.mp4"), src, landscape, r.ShortEdge)
		if err != nil {
			return nil, err
		}
		bytes, segments, err := measureVariant(filepath.Join(outDir, dir))
		if err != nil {
			return nil, err
		}
		if segments == 0 {
			return nil, fmt.Errorf("transcoder: rendition %s produced no segments", r.Name)
		}

		bandwidth := 0
		if src.Duration > 0 {
			bandwidth = int(float64(bytes) * 8 / src.Duration)
		}
		variants = append(variants, HLSVariant{
			Name:      dir,
			Width:     width,
			Height:    height,
			Bandwidth: bandwidth,
			Segments:  segments,
			Bytes:     bytes,
			HasAudio:  src.HasAudio,
		})
	}

	if err := writeMasterPlaylist(filepath.Join(outDir, MasterPlaylistName), src, variants); err != nil {
		return nil, err
	}

	return variants, nil
}

// hlsDir is the on-disk (and later storage) directory for a rendition.
// planRenditions falls back to naming the single native-sized output
// "source"; "native" reads truer in a master playlist that describes
// resolutions, so we rename just the directory.
func hlsDir(name string) string {
	if name == "source" {
		return "native"
	}
	return name
}

// bitrateParams derives burst-control targets from an ffmpeg bitrate string
// like "1400k": maxrate at 1.2x for transient headroom and bufsize at 2x so
// the ABR ladder's advertised bandwidth reflects the encoder's real ceiling.
func bitrateParams(bitrate string) (maxrate, bufsize string, ok bool) {
	if !strings.HasSuffix(bitrate, "k") {
		return "", "", false
	}
	kb, err := strconv.Atoi(strings.TrimSuffix(bitrate, "k"))
	if err != nil || kb <= 0 {
		return "", "", false
	}
	return strconv.Itoa(kb+kb/5) + "k", strconv.Itoa(kb*2) + "k", true
}

// actualDimensions returns the true rendered dimensions of a variant. The
// init.mp4 written by the HLS muxer carries the track metadata ffprobe can
// read; if that probe fails (e.g. odd files), fall back to the same even
// rounding ffmpeg's "-2" scale does.
func actualDimensions(ctx context.Context, f *FFmpeg, initPath string, src hlsSource, landscape bool, shortEdge int) (int, int, error) {
	if info, err := f.probeHLSInfo(ctx, initPath); err == nil && info.Width > 0 && info.Height > 0 {
		return info.Width, info.Height, nil
	}

	var w, h int
	if landscape {
		h = shortEdge
		w = roundEven(float64(src.Width) * float64(shortEdge) / float64(src.Height))
	} else {
		w = shortEdge
		h = roundEven(float64(src.Height) * float64(shortEdge) / float64(src.Width))
	}
	return w, h, nil
}

func roundEven(v float64) int {
	n := int(math.Round(v))
	if n%2 != 0 {
		n++
	}
	return n
}

// measureVariant sums the bytes of every file in a variant dir and counts
// the media segments — the raw material for the honest bandwidth figure.
func measureVariant(dir string) (total int64, segments int, err error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, fmt.Errorf("listing variant dir %s: %w", dir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return 0, 0, fmt.Errorf("statting %s: %w", e.Name(), err)
		}
		total += info.Size()
		if strings.HasSuffix(e.Name(), ".m4s") {
			segments++
		}
	}
	return total, segments, nil
}

// writeMasterPlaylist emits the hand-written master playlist: one
// EXT-X-MEDIA line pointing at the single shared audio playlist, then one
// EXT-X-STREAM-INF line per video variant with the measured bandwidth and
// the codec string for the forced Main@L4.0 encode ("avc1.4d4028" == main
// profile byte 0x4d, constraint byte 0x40, level byte 0x28). Sources higher
// than 1080p@30 would need a higher level (e.g. avc1.4d4029 for L4.2).
//
// Layout requirement: variant/audio playlists are referenced relative to
// the master, and master.m3u8 must live in the SAME directory the worker
// uploads to storage as the root of the HLS tree.
func writeMasterPlaylist(path string, src hlsSource, variants []HLSVariant) error {
	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	b.WriteString("#EXT-X-INDEPENDENT-SEGMENTS\n")

	if src.HasAudio {
		b.WriteString("#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"Audio\",DEFAULT=YES,AUTOSELECT=YES,URI=\"audio/playlist.m3u8\"\n")
	}

	codecs := "avc1.4d4028"
	audioAttr := ""
	if src.HasAudio {
		codecs += ",mp4a.40.2"
		audioAttr = ",AUDIO=\"audio\""
	}
	for _, v := range variants {
		fmt.Fprintf(&b, "#EXT-X-STREAM-INF:BANDWIDTH=%d,AVERAGE-BANDWIDTH=%d,RESOLUTION=%dx%d",
			v.Bandwidth, v.Bandwidth, v.Width, v.Height)
		if src.FPS > 0 {
			fmt.Fprintf(&b, ",FRAME-RATE=%s", strconv.FormatFloat(src.FPS, 'f', -1, 64))
		}
		fmt.Fprintf(&b, ",CODECS=\"%s\"%s\n", codecs, audioAttr)
		fmt.Fprintf(&b, "%s/playlist.m3u8\n", v.Name)
	}

	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return fmt.Errorf("writing master playlist: %w", err)
	}
	return nil
}
