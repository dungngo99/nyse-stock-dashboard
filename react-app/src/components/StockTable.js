export default function StockTable(props) {
    const columns = ['#', 'Exchange', 'First Trade Date', 'Ticker', 'Percent Change', 'Market Price', 'Quote Type', 'Region', 'Time']
    const trending = props.trending;

    return (
        <div className='pb-3'>
            <p>Trending stocks</p>
            <div style={{ height: '50vh', overflow: 'scroll' }}>
                <table className="table">
                    <thead className="thead-dark" style={{margin: '0px', padding: '0px'}}>
                        <tr> {columns.map((item, index) => <th key={item + index} scope='col'>{item}</th>)}</tr>
                    </thead>
                    <tbody>
                        {Object.keys(trending).map((key, index) => {
                            const row = trending[key];
                            return (
                                <tr key={key + index}>
                                    <th scope='row'>{index}</th>
                                    {Object.keys(row).map((key, index) => <td key={key + index}>{row[key]}</td>)}
                                </tr>
                            )
                        })}
                    </tbody>
                </table>
            </div>            
        </div>
    )
}