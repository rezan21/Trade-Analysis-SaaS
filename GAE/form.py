from flask_wtf import FlaskForm
from wtforms import IntegerField, SubmitField, SelectField
from wtforms.validators import DataRequired, NumberRange


class SimpleForm(FlaskForm):        
    Asset = SelectField(u'Asset:', choices=[('AAPL', 'Apple'), ('AMZN', 'Amazon'), ('BABA', 'Alibaba'), ('BTC-GBP', 'Bitcoin')])
    A_Input = IntegerField('A:', render_kw={"placeholder": "Moving Average Period"}, validators=[DataRequired(message="Only Integers Larger Than 2"), NumberRange(min=3, message="Only Integers Larger Than 2")])
    V_Input = IntegerField('V:', render_kw={"placeholder": "Time Window"}, validators=[DataRequired(message="Only Integers Larger Than 2"), NumberRange(min=3, message="Only Integers Larger Than 2")])
    S_Input = IntegerField('S:', render_kw={"placeholder": "MC Samples"}, validators=[DataRequired(message="Only integers between 100 and 1,000,000."), NumberRange(min=100, max=1000000, message="Only integers between 100 and 1,000,000.")])
    R_Input = IntegerField('R:', render_kw={"placeholder": "Resources"}, validators=[DataRequired(message="Only integers between 1 and 10."), NumberRange(min=1, max=10, message="Only integers between 1 and 10.")])
    Res_Type = SelectField(u'Resource', choices=[('ec2', 'AWS - EC2'), ('lambda', 'AWS - Lambda (Serverless)')])

    submit = SubmitField("Analyse")



