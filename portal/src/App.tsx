import { Route, Routes } from "react-router-dom";
import StaffLayout from "./layout/StaffLayout";
import DashboardPage from "./pages/DashboardPage";
import ApplicationDetailPage from "./pages/ApplicationDetailPage";
import NewApplicationPage from "./pages/NewApplicationPage";
import PipelinePage from "./pages/PipelinePage";
import NavigatorPage from "./pages/NavigatorPage";

export default function App() {
  return (
    <Routes>
      <Route element={<StaffLayout />}>
        <Route index element={<DashboardPage />} />
        <Route path="applications/new" element={<NewApplicationPage />} />
        <Route path="applications/:id" element={<ApplicationDetailPage />} />
        <Route path="pipeline" element={<PipelinePage />} />
        <Route path="navigator" element={<NavigatorPage />} />
      </Route>
    </Routes>
  );
}
