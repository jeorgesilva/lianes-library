import { useEffect, useRef, useState } from "react";
import { BrowserMultiFormatReader } from "@zxing/browser";
import type { IScannerControls } from "@zxing/browser";
import { Button } from "./Button";

export function BarcodeScanner({ onDecode }: { onDecode: (text: string) => void }) {
  const [active, setActive] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const videoRef = useRef<HTMLVideoElement>(null);
  const controlsRef = useRef<IScannerControls | null>(null);

  useEffect(() => {
    if (!active || !videoRef.current) return;

    const reader = new BrowserMultiFormatReader();
    let cancelled = false;

    reader
      .decodeFromVideoDevice(undefined, videoRef.current, (result, _err, controls) => {
        controlsRef.current = controls;
        if (cancelled) return;
        if (result) {
          onDecode(result.getText());
          controls.stop();
          setActive(false);
        }
        // NotFoundException fires continuously while no barcode is in view — not a real error.
      })
      .catch((e) => setError(e instanceof Error ? e.message : "Could not access camera"));

    return () => {
      cancelled = true;
      controlsRef.current?.stop();
    };
  }, [active, onDecode]);

  return (
    <div className="flex flex-col items-start gap-2">
      {!active ? (
        <Button type="button" variant="secondary" onClick={() => { setError(null); setActive(true); }}>
          📸 Scan barcode
        </Button>
      ) : (
        <Button type="button" variant="secondary" onClick={() => setActive(false)}>
          ❌ Turn off camera
        </Button>
      )}
      {error && <p className="text-sm text-danger">{error}</p>}
      {active && (
        <video
          ref={videoRef}
          className="w-full max-w-sm rounded-lg border border-border"
          muted
          playsInline
        />
      )}
    </div>
  );
}
