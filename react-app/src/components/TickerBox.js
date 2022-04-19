import { Line } from 'react-chartjs-2';
import { CategoryScale } from 'chart.js';
import Chart from 'chart.js/auto'

Chart.register(CategoryScale)
export default function TickerBox(props) {
    const ts = props.data['timeseries'];
    const profile = props.data['profile'];

    const createChart = (ts) => {
        const labels = Array.from(Array(ts['datetime'].length), () => "");
        const data = {
            labels,
            datasets: [
                {
                    data: ts['open'],
                    pointRadius: 0,
                }
            ],
        };
        const options = {
            responsive: true,
            backgroundColor: 'blue',
            fill: true,
            spanGaps: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false
                }
            },
            pointStyle: 'line',
            scales: {
                x: {
                    display: false,
                    grid: {
                        display: false
                    }
                },
                y: {
                    display: false,
                    grid: {
                        display: false
                    }
                }
            }
        }
        return [data, options];
    }
    const [data, options] = createChart(ts);

    return (
        <div className="card text-center">
            <div className="card-header">
                <div className='d-flex justify-content-between' >
                    <button type="button" className="btn btn-light btn-rounded btn-sm">
                        {props.name}
                    </button>
                    <p style={{ margin: '0px' }}>${profile['openPrice']}</p>
                </div>
                <div>
                    <p style={{ margin: '0px' }}>{profile['name']}</p>
                </div>
            </div>

            <div className="card-body">
                <td className='p-0 m-0' style={{ maxHeight: '50px', maxWidth: '150px' }}>
                    <Line options={options} data={data} />
                </td>
                <button href="." className="mt-1 mb-0 p-1 btn btn-primary">Watch</button>
            </div>
            <div className="card-footer text-muted">Market cap: ${profile['marketCap']}</div>
        </div>
    )
}