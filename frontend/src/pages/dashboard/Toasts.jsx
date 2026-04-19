import { useCallback, useEffect, useRef, useState } from "react";

export function useToasts() {
  const [toasts, setToasts] = useState([]);
  const idRef = useRef(0);

  const push = useCallback((t) => {
    const id = ++idRef.current;
    const toast = typeof t === "string" ? { title: t } : t;
    const full = { id, kind: "info", ttl: 3800, ...toast };
    setToasts((ts) => [...ts, full]);
    if (full.ttl > 0) {
      setTimeout(() => setToasts((ts) => ts.filter((x) => x.id !== id)), full.ttl);
    }
    return id;
  }, []);

  const update = useCallback((id, patch) => {
    setToasts((ts) => ts.map((t) => (t.id === id ? { ...t, ...patch } : t)));
  }, []);

  const dismiss = useCallback((id) => {
    setToasts((ts) => ts.filter((t) => t.id !== id));
  }, []);

  useEffect(() => {
    window.notify = push;
    return () => {
      if (window.notify === push) delete window.notify;
    };
  }, [push]);

  return { toasts, push, update, dismiss };
}

export function Toasts({ toasts, dismiss }) {
  return (
    <div className="toast-stack">
      {toasts.map((t) => (
        <div key={t.id} className={`toast toast-${t.kind}`}>
          <div className="toast-row">
            <span className={`toast-led led-${t.kind}`} />
            <div className="toast-body">
              <div className="toast-title">{t.title}</div>
              {t.body && <div className="toast-sub">{t.body}</div>}
              {typeof t.progress === "number" && (
                <div className="toast-bar">
                  <div
                    className="fill"
                    style={{ width: Math.min(100, t.progress) + "%" }}
                  />
                </div>
              )}
            </div>
            <button className="toast-x" onClick={() => dismiss(t.id)}>
              ×
            </button>
          </div>
        </div>
      ))}
    </div>
  );
}
