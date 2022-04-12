import './App.css';
import Trending from './pages/trending';
import { Analyzer } from './pages/analyzer';
import 'mdb-react-ui-kit/dist/css/mdb.min.css'

function App() {
  return (
    <div className="App">
      <Trending></Trending>
      <Analyzer></Analyzer>
    </div>
  );
}

export default App;
