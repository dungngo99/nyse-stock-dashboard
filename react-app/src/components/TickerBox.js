export default function TickerBox(props) {
  return (
    <div className="card text-center">
      <div className="card-header">
        <div className='d-flex justify-content-between'>
          <button type="button" className="btn btn-light btn-rounded btn-sm">
            <i className="fab fa-apple"></i> {props.name}
          </button>
          <div>
            <p style={{ margin: '5px' }}>{props.name}</p>
            <p style={{ margin: '0px' }}>1000.0</p>
          </div>
        </div>
      </div>
      <div className="card-body">
        <p className="card-text">Grah here</p>
        <button href="." className="btn btn-primary">Watch</button>
      </div>
      <div className="card-footer text-muted">Market price ...</div>
    </div>
  )
}