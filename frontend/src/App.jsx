import { Component, useState } from "react";
import { DashboardPage } from "./pages/DashboardPage";
import { FileDrawer } from "./components/FileDrawer";

class ErrorBoundary extends Component {
  constructor(p) {
    super(p);
    this.state = { err: null, info: null };
  }
  static getDerivedStateFromError(err) {
    return { err };
  }
  componentDidCatch(err, info) {
    this.setState({ err, info });
    if (typeof window !== "undefined") {
      window.__lastReactError = { message: err?.message, stack: err?.stack, componentStack: info?.componentStack };
    }
  }
  render() {
    if (this.state.err) {
      return (
        <div style={{ padding: 24, fontFamily: "system-ui", color: "#fff", background: "#1a0a0a", minHeight: "100vh" }}>
          <h2 style={{ color: "#ef4444" }}>UI crashed — full error below</h2>
          <pre style={{ whiteSpace: "pre-wrap", fontSize: 12 }}>
            {String(this.state.err?.message)}
            {"\n\n"}
            {String(this.state.err?.stack || "").slice(0, 2000)}
            {"\n\n— Component stack —"}
            {String(this.state.info?.componentStack || "").slice(0, 2000)}
          </pre>
        </div>
      );
    }
    return this.props.children;
  }
}

export default function App() {
  const [drawerPath, setDrawerPath] = useState(null);
  return (
    <ErrorBoundary>
      <DashboardPage onFileClick={setDrawerPath} />
      {drawerPath && <FileDrawer path={drawerPath} onClose={() => setDrawerPath(null)} />}
    </ErrorBoundary>
  );
}
