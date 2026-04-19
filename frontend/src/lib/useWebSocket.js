import { useState, useEffect, useRef, useCallback } from "react";

/**
 * WebSocket hook that connects to the pipeline server for live updates.
 * Falls back to polling if WebSocket is unavailable.
 */
export function useWebSocket(fallbackFetcher, fallbackIntervalMs = 3000) {
  const [pipeline, setPipeline] = useState(null);
  const [gpu, setGpu] = useState(null);
  const [control, setControl] = useState(null);
  const [connected, setConnected] = useState(false);
  const wsRef = useRef(null);
  const reconnectTimer = useRef(null);
  const reconnectDelay = useRef(1000);
  const fallbackFetcherRef = useRef(fallbackFetcher);
  fallbackFetcherRef.current = fallbackFetcher;

  const connect = useCallback(() => {
    // Derive WS URL from current page location
    const proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    const url = `${proto}//${window.location.host}/api/ws`;

    try {
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = async () => {
        setConnected(true);
        reconnectDelay.current = 1000; // reset backoff
        // Belt-and-braces: the backend's WS now sends state on connect AND heartbeats every
        // ~6s, but fire an HTTP snapshot anyway so the dashboard isn't blank for a moment
        // while we wait for the first WS push after (re)connect.
        try {
          const fresh = await fallbackFetcherRef.current?.();
          if (fresh) setPipeline(fresh);
        } catch {}
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(event.data);
          if (msg.type === "pipeline") setPipeline(msg.data);
          else if (msg.type === "gpu") setGpu(msg.data);
          else if (msg.type === "control") setControl(msg.data);
        } catch {
          // ignore parse errors
        }
      };

      ws.onclose = () => {
        setConnected(false);
        wsRef.current = null;
        // Reconnect with exponential backoff (max 30s)
        reconnectTimer.current = setTimeout(() => {
          reconnectDelay.current = Math.min(reconnectDelay.current * 1.5, 30000);
          connect();
        }, reconnectDelay.current);
      };

      ws.onerror = () => {
        ws.close();
      };
    } catch {
      setConnected(false);
    }
  }, []);

  useEffect(() => {
    connect();
    return () => {
      if (wsRef.current) wsRef.current.close();
      if (reconnectTimer.current) clearTimeout(reconnectTimer.current);
    };
  }, [connect]);

  // Fallback polling when WS is disconnected
  useEffect(() => {
    if (connected || !fallbackFetcher) return;
    const poll = async () => {
      try {
        const data = await fallbackFetcher();
        setPipeline(data);
      } catch {
        // ignore
      }
    };
    poll();
    const id = setInterval(poll, fallbackIntervalMs);
    return () => clearInterval(id);
  }, [connected, fallbackFetcher, fallbackIntervalMs]);

  return { pipeline, gpu, control, connected };
}
