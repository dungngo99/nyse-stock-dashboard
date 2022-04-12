import { useState } from 'react'
import Pagination from 'react-bootstrap/Pagination'
import Table from 'react-bootstrap/Table'
import { Line } from 'react-chartjs-2';
import { CategoryScale } from 'chart.js';
import Chart from 'chart.js/auto'

Chart.register(CategoryScale)

export default function StockTable(props) {
    const [index, setIndex] = useState(0);
    const columns = ['#', 'Exchange', 'Ticker', 'Percent Change', 'Market Price', 'Quote Type', 'Time', 'Chart']

    const createPagination = () => {
        const itemsPerPagination = 5;
        let list = [];
        let ts = []

        Object.keys(props.trending).forEach((key, index) => {
            const data = props.trending[key];

            if (index % itemsPerPagination === 0) {
                list.push([]);
                ts.push([]);
            }

            ts[ts.length - 1].push(data['timeseries']);
            list[list.length - 1].push({
                exchange: data['exchange'],
                name: data['name'],
                percent: data['percent'],
                price: data['price'],
                quoteType: data['quoteType'],
                time: data['time']
            });
        })
        return [list, ts];
    }
    const [stocks, ts] = createPagination();

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

    return (
        <div className='pb-3 d-flex flex-column'>
            <p>Trending stocks</p>

            <div style={{ height: '45vh', overflow: 'hidden' }}>
                <Table className="table" size='sm' hover={true}>
                    <thead className="thead-dark m-0 p-0" style={{ position: '-webkit - sticky', position: 'sticky', top: '0px' }}>
                        <tr style={{ textAlign: 'center' }}> {columns.map((item, index) => <th className='m-0 p-0' key={item + index} scope='col'>{item}</th>)}</tr>
                    </thead>

                    <tbody className='m-0 p-0'>
                        {stocks.length !== 0 ? stocks[index].map((row, i) => {
                            const color = row['percent'] < 0 ? '#F65555' : 'green';
                            const [data, options] = createChart(ts[index][i]);
                            options['backgroundColor'] = row['percent'] < 0 ? '#F65555' : '#75F745';

                            return (
                                <tr key={row + i} style={{ color: color}}>
                                    <th className='p-0 p-0' scope='row'>{i + 1}</th>
                                    {Object.keys(row).map((key, ii) => <td className='p-0 p-0' key={key + ii}>{row[key]}</td>)}
                                    <td className='p-0 p-0' style={{ maxHeight: '75px', maxWidth: '350px'}}>
                                        <Line options={options} plugins={{}} data={data} />
                                    </td>
                                </tr>

                            )
                        }) : null}
                    </tbody>
                </Table>
            </div>

            <Pagination className='align-self-end m-0 p-0 pagination-lg '>
                <Pagination.First onClick={() => setIndex(0)} />
                <Pagination.Prev onClick={() => setIndex(index - 1 < 0 ? 0 : index - 1)} />
                {index >= 1 ? <Pagination.Item >{1}</Pagination.Item> : null}
                {index >= 2 ? <Pagination.Ellipsis /> : null}

                <Pagination.Item onClick={() => setIndex(1)}>{index + 1}</Pagination.Item>

                {index <= stocks.length - 3 ? <Pagination.Ellipsis /> : null}
                {index <= stocks.length - 2 ? <Pagination.Item >{stocks.length}</Pagination.Item> : null}
                <Pagination.Next onClick={() => setIndex(index + 1 === stocks.length ? stocks.length - 1 : index + 1)} />
                <Pagination.Last onClick={() => setIndex(stocks.length - 1)} />
            </Pagination>
        </div>
    )
}